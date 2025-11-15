package connector

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MathewBravo/cdc-pipeline/internal/configs"
	"github.com/MathewBravo/cdc-pipeline/internal/events"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type PostgresConnector struct {
	config          configs.SourceConfig
	replConn        *pgconn.PgConn
	eventChan       chan events.ChangeEvent
	stopChan        chan struct{}
	wg              sync.WaitGroup
	relationCache   map[uint32]pglogrepl.RelationMessage
	lastRecievedLSN pglogrepl.LSN
}

func NewPGConnector(cfg configs.SourceConfig) *PostgresConnector {
	return &PostgresConnector{
		config:        cfg,
		eventChan:     make(chan events.ChangeEvent, 100),
		stopChan:      make(chan struct{}),
		relationCache: make(map[uint32]pglogrepl.RelationMessage),
	}
}

func (p *PostgresConnector) Start() (<-chan events.ChangeEvent, error) {
	fmt.Println("DEBUG: Building postgres connection string...")
	connStr := p.buildConnString()
	fmt.Printf("DEBUG: Connection string: %s\n", connStr)

	fmt.Println("DEBUG: Attempting to connect...")
	replConn, err := pgconn.Connect(context.Background(), connStr)
	if err != nil {
		return nil, fmt.Errorf("CONN ERR: failed to connect: %w", err)
	}
	fmt.Println("DEBUG: Successfully connected!")

	p.replConn = replConn

	fmt.Println("DEBUG: Starting replication goroutine...")
	p.wg.Add(1)
	go p.replicationLoop()

	fmt.Println("DEBUG: Returning event channel...")
	return p.eventChan, nil
}

func (p *PostgresConnector) Stop() error {
	p.stopChan <- struct{}{}
	p.replConn.Close(context.Background())
	p.wg.Wait()
	close(p.eventChan)
	return nil
}

func (p *PostgresConnector) buildConnString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database", p.config.User, p.config.Password, p.config.Host, p.config.Port, p.config.Database)
}

func (p *PostgresConnector) replicationLoop() {
	defer p.wg.Done()

	fmt.Println("DEBUG: Beginning replication loop...")
	ctx := context.Background()

	lsn, err := fetchLastLSN()
	if err != nil {
		fmt.Println("LSN ERR: Error fetching last LSN: ", err)
		return
	}

	p.lastRecievedLSN = lsn

	fmt.Println("DEBUG: Starting logical replication...")

	opts := pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", p.config.PublicationName),
		},
	}

	if err := pglogrepl.StartReplication(ctx, p.replConn, p.config.SlotName, p.lastRecievedLSN, opts); err != nil {
		fmt.Printf("REPLICATION ERROR: failed to start replication: %v\n", err)
		return
	}

	fmt.Println("Replication Successfully started!")

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopChan:
			fmt.Println("Stop signal received")
			p.replConn.Close(context.Background())
			return
		case <-ticker.C:
			// TODO: Send standby status update
			fmt.Println("Heartbeat tick")
			p.sendStatusUpdate()
		default:
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			msg, err := p.replConn.ReceiveMessage(ctx)
			cancel()
			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				fmt.Printf("Error receiving message: %v\n", err)
				return
			}
			p.ReadMessage(msg)
		}
	}
}

func (p *PostgresConnector) ReadMessage(bm pgproto3.BackendMessage) {
	copyD, ok := bm.(*pgproto3.CopyData)
	if !ok {
		fmt.Println("ReceiveMessage that was not CopyData message")
		return
	}

	data := copyD.Data
	if len(data) < 1 {
		fmt.Println("Recieved empty data message")
		return
	}

	msgType := data[0]
	switch msgType {
	case 'w':
		xlog, err := pglogrepl.ParseXLogData(data[1:])
		if err != nil {
			fmt.Println("Could not parse XlogData: ", err)
			return
		}

		p.lastRecievedLSN = xlog.WALStart
		wd, err := pglogrepl.Parse(xlog.WALData)
		if err != nil {
			fmt.Println("PARSE ERR: Could not parse WAL data: ", err)
		}

		p.handleLogicalReplicationMessage(wd)
	case 'k':
		p.sendStatusUpdate()
	default:
		fmt.Printf("Unknown replication message type: %c (0x%02x)\n", msgType, msgType)
	}
}

func (p *PostgresConnector) handleLogicalReplicationMessage(walMessage pglogrepl.Message) {
	switch walMessage.Type() {
	case pglogrepl.MessageTypeRelation:
		relationMsg, ok := walMessage.(*pglogrepl.RelationMessage)
		if !ok {
			return
		}
		p.relationCache[relationMsg.RelationID] = *relationMsg
		return
	case pglogrepl.MessageTypeUpdate:
		updateMsg, ok := walMessage.(*pglogrepl.UpdateMessage)
		if !ok {
			return
		}
		relMsg := p.relationCache[updateMsg.RelationID]

		oldData := parseTupleData(updateMsg.OldTuple, &relMsg)
		newData := parseTupleData(updateMsg.NewTuple, &relMsg)

		ce := events.ChangeEvent{
			Operation: events.OperationUpdate,
			NameSpace: relMsg.Namespace,
			Table:     relMsg.RelationName,
			Before:    oldData,
			After:     newData,
			Lsn:       p.lastRecievedLSN.String(),
			PK:        getPKColumns(&relMsg),
		}

		fmt.Println(ce.Pretty())
		p.eventChan <- ce
		return
	case pglogrepl.MessageTypeDelete:
		deleteMsg, ok := walMessage.(*pglogrepl.DeleteMessage)
		if !ok {
			return
		}
		relMsg := p.relationCache[deleteMsg.RelationID]

		delData := parseTupleData(deleteMsg.OldTuple, &relMsg)

		ce := events.ChangeEvent{
			Operation: events.OperationDelete,
			NameSpace: relMsg.Namespace,
			Table:     relMsg.RelationName,
			Before:    delData,
			After:     nil,
			Lsn:       p.lastRecievedLSN.String(),
			PK:        getPKColumns(&relMsg),
		}

		fmt.Println(ce.Pretty())
		p.eventChan <- ce
		return
	case pglogrepl.MessageTypeInsert:
		insertMsg, ok := walMessage.(*pglogrepl.InsertMessage)
		if !ok {
			return
		}
		relMsg := p.relationCache[insertMsg.RelationID]

		newData := parseTupleData(insertMsg.Tuple, &relMsg)

		ce := events.ChangeEvent{
			Operation: events.OperationInsert,
			NameSpace: relMsg.Namespace,
			Table:     relMsg.RelationName,
			Before:    nil,
			After:     newData,
			Lsn:       p.lastRecievedLSN.String(),
			PK:        getPKColumns(&relMsg),
		}

		fmt.Println(ce.Pretty())
		p.eventChan <- ce
		return

	case pglogrepl.MessageTypeCommit:
		lsnStr := p.lastRecievedLSN.String()
		fmt.Println("DEBUG: Writing lsn log")
		logLSN(lsnStr)

	default:
		fmt.Println("MESSAGE TYPE: ", walMessage.Type().String())
	}
}

func parseTupleData(tupleData *pglogrepl.TupleData, relationMsg *pglogrepl.RelationMessage) map[string]any {
	if tupleData == nil {
		return nil
	}
	result := make(map[string]any)
	for i := range len(tupleData.Columns) {
		colName := relationMsg.Columns[i].Name
		colTypeOID := relationMsg.Columns[i].DataType
		tupleCol := tupleData.Columns[i]

		switch tupleCol.DataType {
		case 'n':
			result[colName] = nil
		case 't':
			data := parseByOID(tupleCol.Data, colTypeOID)
			result[colName] = data
		default:
			fmt.Println("UNSUPPORTED TUPLE DATATYPE")
		}
	}

	return result
}

func parseByOID(data []byte, oid uint32) any {
	text := string(data)
	switch oid {
	case 23: // int4
		val, _ := strconv.ParseInt(text, 10, 32)
		return int32(val)
	case 20: // int8 (bigint)
		val, _ := strconv.ParseInt(text, 10, 64)
		return val
	case 25, 1043: // text, varchar
		return text
	case 16: // bool
		return text == "t" || text == "true"
	case 1114: // timestamp without timezone
		t, _ := time.Parse("2006-01-02 15:04:05", text)
		return t
	case 1184: // timestamp with timezone
		t, _ := time.Parse("2006-01-02 15:04:05.999999-07", text)
		return t
	case 1700: // numeric/decimal
		val, _ := strconv.ParseFloat(text, 64)
		return val
	case 701: // float8
		val, _ := strconv.ParseFloat(text, 64)
		return val
	default:
		return text
	}
}

func (p *PostgresConnector) sendStatusUpdate() {
	err := pglogrepl.SendStandbyStatusUpdate(context.Background(), p.replConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: p.lastRecievedLSN,
		WALFlushPosition: p.lastRecievedLSN,
		WALApplyPosition: p.lastRecievedLSN,
		ClientTime:       time.Now(),
		ReplyRequested:   false,
	})
	if err != nil {
		fmt.Println("ERR: Could not send standby status: ", err)
	}
}

func logLSN(lsn string) {
	configDir, err := os.UserConfigDir()
	if err != nil {
		fmt.Println("OS ERR: Could not find log file dir: ", err)
		return
	}
	logFile := configDir + "/cdc-pipe/log.txt"
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		fmt.Println("OS ERR: Could not open log.txt", err)
		return
	}
	defer f.Close()

	_, err = f.WriteString(lsn)
	if err != nil {
		fmt.Println("OS ERR: Could not write LSN", err)
		return
	}
}

func fetchLastLSN() (pglogrepl.LSN, error) {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return 0, err
	}

	logFile := configDir + "/cdc-pipe/log.txt"

	data, err := os.ReadFile(logFile)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	lsnStr := string(data)
	lsnStr = strings.TrimSpace(lsnStr)

	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		return 0, err
	}

	return lsn, nil
}

func getPKColumns(relMSG *pglogrepl.RelationMessage) []string {
	var pkCols []string
	for _, col := range relMSG.Columns {
		if col.Flags == 1 {
			pkCols = append(pkCols, col.Name)
		}
	}
	return pkCols
}
