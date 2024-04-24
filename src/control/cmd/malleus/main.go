package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/daos-stack/daos/src/control/drpc"
	"github.com/daos-stack/daos/src/control/security/auth"
)

func logMsg(logger *log.Logger, msg string) {
	logger.Printf(msg)
}

func logThread(logger *log.Logger, threadNum int, msg string) {
	logMsg(logger, fmt.Sprintf("thread %d: %s", threadNum, msg))
}

func main() {
	fmt.Println("===========================================================")
	fmt.Println("           T   H   E      H   A   M   M   E   R")
	fmt.Println("===========================================================")

	sock := "/tmp/daos_sockets/daos_agent.sock"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	failure := make(chan error)

	numThreads := 100

	execName := filepath.Base(os.Args[0])
	logger := log.New(os.Stdout, execName+": ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lmsgprefix)

	for i := 0; i < numThreads; i++ {
		go func(threadNum int) {
			logThread(logger, threadNum, "starting...")
			client := drpc.NewClientConnection(sock)
			for j := 0; ; j++ {
				select {
				case <-ctx.Done():
					logThread(logger, threadNum, "context done, exiting...")
					return
				default:
					if err := sendReq(ctx, threadNum, j, client); err != nil {
						logThread(logger, threadNum, fmt.Sprintf("failed on round %d, exiting...", j))
						failure <- err
						return
					}
				}
			}
		}(i)
	}

	select {
	case err := <-failure:
		logMsg(logger, "first failure: "+err.Error())
	case <-ctx.Done():
		logMsg(logger, "context canceled: "+ctx.Err().Error())
	}
}

func sendReq(ctx context.Context, threadNum, seq int, client drpc.DomainSocketClient) error {
	// logThread(threadNum, "connecting")
	if err := client.Connect(ctx); err != nil {
		return errors.Wrapf(err, "thread %d: Connect", threadNum)
	}

	call := &drpc.Call{
		Module:   drpc.ModuleSecurityAgent.ID(),
		Method:   drpc.MethodRequestCredentials.ID(),
		Sequence: int64(seq),
	}

	// logThread(threadNum, "sending dRPC req")
	resp, err := client.SendMsg(ctx, call)
	if err != nil {
		return errors.Wrapf(err, "thread %d: SendMsg", threadNum)
	}

	if resp.Status != 0 {
		return errors.Errorf("thread %d: bad resp status: %s(%d)", threadNum, resp.Status, resp.Status)
	}

	pbResp := new(auth.GetCredResp)
	if err := proto.Unmarshal(resp.Body, pbResp); err != nil {
		return errors.Wrapf(drpc.UnmarshalingPayloadFailure(), "thread %d", threadNum)
	}

	if pbResp.Cred == nil {
		return errors.Errorf("thread %d: nil cred", threadNum)
	}

	pbCred := new(auth.Sys)
	if err := proto.Unmarshal(pbResp.Cred.Token.Data, pbCred); err != nil {
		return errors.Wrapf(err, "thread %d: unmarshal token", threadNum)
	}

	// logThread(threadNum, fmt.Sprintf("credential for %s/%s received", pbCred.User, pbCred.Group))
	return nil
}
