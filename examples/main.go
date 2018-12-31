package main

import (
	"fmt"
	"github.com/saromanov/redis-cron"
)

func main(){
	a := cron.New(&cron.ClientOptions{})
	a.AddTrigger("test", &cron.Trigger{

	})
	a.Start()
}