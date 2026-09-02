package GrowRPC

import (
	"fmt"
	"html/template"
	"net/http"
)

const debugText = `<html>
	<body>
	<title>GrowRPC Services</title>
	<hr>
	Service Methods
	<hr>
		<ul>
		{{range .}}
			<li>{{.}}</li>
		{{end}}
		</ul>
	</body>
	</html>`

var debug = template.Must(template.New("RPC debug").Parse(debugText))

type debugHTTP struct {
	*Server
}

// Runs at /debug/geerpc
func (server debugHTTP) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	// Build a sorted version of the data.
	var methods []string
	server.serviceMap.Range(func(namei, svci interface{}) bool {
		methods = append(methods, namei.(string))
		return true
	})
	err := debug.Execute(w, methods)
	if err != nil {
		_, _ = fmt.Fprintln(w, "rpc: error executing template:", err.Error())
	}
}
