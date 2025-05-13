package helm

import (
	"embed"
)

var (
	//go:embed all:kgateway
	KgatewayHelmChart embed.FS
)
