package main

import (
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gctx"
	"github.com/mldong/jeeflow-go/demo"
)

func main() {
	ctx := gctx.New()
	s := g.Server()
	s.SetPort(8081)
	// CORS——允许 jeeflow-ui (localhost:5173) 跨域访问
	s.SetConfigWithMap(g.Map{
		"Cors": g.Map{
			"AllowOrigin":  "*",
			"AllowMethods": "POST, GET, OPTIONS, PUT, DELETE",
			"AllowHeaders": "Content-Type, Authorization",
		},
	})

	ctl := demo.New()
	ctl.RegisterRoutes(s)

	s.Run()
	_ = ctx
}
