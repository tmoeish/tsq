module github.com/tmoeish/tsq

retract [v1.0.0, v1.0.20] // 撤回所有误发的 v1.0.x 版本

go 1.24.2

require (
	github.com/juju/errors v1.0.0
	github.com/serenize/snaker v0.0.0-20201027110005-a7ad2135616e
	github.com/spf13/cobra v1.10.2
	github.com/stretchr/testify v1.11.1
	golang.org/x/tools v0.38.0
	gopkg.in/nullbio/null.v6 v6.0.0-20161116030900-40264a2e6b79
)

require (
	github.com/google/go-cmp v0.7.0 // indirect
	golang.org/x/mod v0.29.0 // indirect
	golang.org/x/sync v0.17.0 // indirect
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/mattn/go-sqlite3 v1.14.42
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/onsi/gomega v1.33.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	mvdan.cc/gofumpt v0.9.2
)
