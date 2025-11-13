package init

import (
	"fmt"
	"os"
)

func Init() {
	configDir, err := os.UserConfigDir()
	if err != nil {
		panic("No user config directory set")
	}

	cdcPipeDir := configDir + "/cdc-pipe"
	err = os.MkdirAll(cdcPipeDir, 0o755)
	if err != nil {
		fmt.Println("ERR: Could not make init dir: ", err)
		return
	}
	fmt.Println(cdcPipeDir)
	initializeLSNStore(cdcPipeDir)
}

func initializeLSNStore(dir string) {
	logFilePath := dir + "/log.txt"
	_, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fmt.Println("Could not initilize file")
		return
	}
}
