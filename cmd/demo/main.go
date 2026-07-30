package main

import (
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gctx"
	"github.com/mldong/jeeflow-go/demo"
)

func main() {
	ctx := gctx.New()
	s := g.Server()
	s.SetServerRoot("demo/web")
	s.SetPort(8081)

	ctl := demo.New()
	ctl.RegisterRoutes(s)

	s.Run()
	_ = ctx
}
