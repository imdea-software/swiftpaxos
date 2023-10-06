package hook

import (
	"os"
	"os/signal"
	"syscall"
)

func HookUser1(f func()) {
	user1 := make(chan os.Signal, 1)
	signal.Notify(user1, syscall.SIGUSR1)

	go func() {
		<-user1
		f()
	}()
}
