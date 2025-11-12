// TODO: Implement logical replication message decoding.
// Currently the connector receives raw *pgproto3.CopyData frames from Postgres.
// Next steps:
//  1. Detect XLogData vs PrimaryKeepaliveMessage frames (m.Data[0]).
//  2. Parse XLogData using pglogrepl.ParseXLogData.
//  3. Decode the WALData payload with pglogrepl.Parse to extract logical messages
//     (Begin, Relation, Insert, Update, Delete, Commit).
//  4. Map these messages into events.ChangeEvent structs and push them to eventChan.
//  5. (Later) Send periodic standby status updates to prevent connection timeout.

// Resources For Writeup
// https://www.postgresql.org/docs/16//protocol-replication.html
// https://packagemain.tech/p/real-time-database-change-tracking
// https://pkg.go.dev/github.com/jackc/pglogrepl
// https://pkg.go.dev/github.com/jackc/pgx/v5/pgproto3
// https://pkg.go.dev/github.com/jackc/pgx/v5/pgconn

package connector

import (
	"context"
	"fmt"
	"time"

	"github.com/MathewBravo/cdc-pipeline/internal/configs"
	"github.com/MathewBravo/cdc-pipeline/internal/events"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type PostgresConnector struct {
	// config, db connection, etc.
	config    configs.SourceConfig
	replConn  *pgconn.PgConn
	eventChan chan events.ChangeEvent
	stopChan  chan struct{}
}

type PGMessage struct{}

func NewPGConnector(cfg configs.SourceConfig) *PostgresConnector {
	return &PostgresConnector{
		config:    cfg,
		eventChan: make(chan events.ChangeEvent, 100),
		stopChan:  make(chan struct{}),
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
	go p.replicationLoop()

	fmt.Println("DEBUG: Returning event channel...")
	return p.eventChan, nil
}

func (p *PostgresConnector) Stop() error {
	// 1. Signal goroutine to stop
	// 2. Close channel
	// 3. Cleanup resources
	return nil
}

func (p *PostgresConnector) buildConnString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database", p.config.User, p.config.Password, p.config.Host, p.config.Port, p.config.Database)
}

func (p *PostgresConnector) replicationLoop() {
	fmt.Println("DEBUG: Beginning replication loop...")
	ctx := context.Background()

	startLSN := pglogrepl.LSN(0)

	fmt.Println("DEBUG: Starting logical replication...")

	opts := pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", p.config.PublicationName),
		},
	}

	if err := pglogrepl.StartReplication(ctx, p.replConn, p.config.SlotName, startLSN, opts); err != nil {
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
			return
		case <-ticker.C:
			// TODO: Send standby status update
			fmt.Println("Heartbeat tick")
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
			ReadMessage(msg)
			fmt.Printf("Received message: %T\n", msg)
		}
	}
}

func ReadMessage(bm pgproto3.BackendMessage) {
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
		fmt.Println("WAL")
		xlog, err := pglogrepl.ParseXLogData(data[1:])
		if err != nil {
			fmt.Println("Could not parse XlogData: ", err)
			return
		}
		fmt.Println("XlogData: WAL")

		fmt.Println("time: ", xlog.ServerTime.UTC().String())
		wd, err := pglogrepl.Parse(xlog.WALData)
		if err != nil {
			fmt.Println("PARSE ERR: Could not parse WAL data: ", err)
		}
		handleLogicalReplicationMessage(wd)

	case 'k':
		fmt.Println("KEEP ALIVE")
	default:
		fmt.Printf("Unknown replication message type: %c (0x%02x)\n", msgType, msgType)
	}
}

func handleLogicalReplicationMessage(walMessage pglogrepl.Message) {
	fmt.Println("Message Type: ", walMessage.Type().String())
}
