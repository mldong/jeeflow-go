package main

import (
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/net/ghttp"
	"github.com/gogf/gf/v2/os/gctx"
	"github.com/mldong/jeeflow-go/demo"
)

func main() {
	ctx := gctx.New()
	s := g.Server()
	s.SetPort(8081)

	// CORS——允许 jeeflow-ui (localhost:5173) 跨域直连
	// 注意：ServerConfig 的 Cors 配置对未注册路由的 OPTIONS 预检不生效，
	// 这里用全局中间件统一处理（含 OPTIONS 短路）。
	s.BindMiddlewareDefault(func(r *ghttp.Request) {
		r.Response.Header().Set("Access-Control-Allow-Origin", "*")
		r.Response.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		r.Response.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == "OPTIONS" {
			r.Response.WriteStatusExit(200)
		}
		r.Middleware.Next()
	})

	ctl := demo.New()
	ctl.RegisterRoutes(s)

	s.Run()
	_ = ctx
}
