module github.com/prologic/bitraft

require (
	github.com/golang/leveldb v0.0.0-20170107010102-259d9253d719 // indirect
	github.com/hashicorp/go-sockaddr v1.0.2
	github.com/onsi/ginkgo v1.8.0 // indirect
	github.com/onsi/gomega v1.5.0 // indirect
	github.com/prologic/bitcask v0.3.10
	github.com/sirupsen/logrus v1.8.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/tidwall/finn v0.1.2
	github.com/tidwall/redcon v1.4.0
	golang.org/x/exp v0.0.0-20201008143054-e3b2a7f2fdc7 // indirect
	golang.org/x/net v0.0.0-20190724013045-ca1201d0de80 // indirect
	golang.org/x/sys v0.0.0-20201118182958-a01c418693c7 // indirect
)

go 1.13

replace labix.org/v2/mgo => gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22

replace launchpad.net/gocheck => github.com/go-check/check v0.0.0-20190902080502-41f04d3bba15
