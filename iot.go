//package main

package iot

/*
go get -u all
go get -u github.com/godoctor/godoctor
go get github.com/tkanos/gonfig
*/

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type IotPayload struct {
	Cmd       string
	Parms     []string
	Id        string
	NodeId    int
	PropId    int
	VarId     int
	ConnId    int
	Val       string //int64
	Timestamp int64
	Start     int64
	Stop      int64
	Length    int // ??
	Error     string
	IsJson    bool
}

type IotNode struct {
	NodeId      int
	ConnId      int
	Name        string
	NodeType    int
	FreeMem     int
	BootCount   int
	BootStamp   int64
	Timestamp   int64
	IsInit      bool
	IsDefSaved  bool
	IsDataSaved bool
	IsOffline   bool
	Props       map[int]*IotProp
}

// IotProp ...
type IotProp struct {
	Name   string
	NodeId int
	PropId int
	Node   *IotNode

	// FromDb      bool
	IsInit      bool
	IsDefSaved  bool
	IsDataSaved bool
	IsStr       bool
	OnRead      bool
	OnSave      bool

	Decimals    int    // & 0,1,2,3, <0=string
	LogOption   int    // & (logMem) 0=none, 1=skip equals, 2=replace extrapolated values
	TraceOption int    // & (iotTrace) 0=none, 1=trace,
	LocalMvc    bool   // -
	GlobalMvc   bool   // -
	Publish     string //

	Val       int64 // &
	ValString string
	ValStamp  int64 // &
	Prev      int64 // &
	PrevStamp int64 // &
	// Soll           	int
	// SollTimeStamp  	int64
	Err      string // &
	ErrStamp int64
	InErr    bool // -

	DrawType   int
	DrawColor  string
	DrawOffset int
	DrawFactor float32

	RefreshRate int
	RetryCount  int
	NextRefresh int64
}

var Nodes map[int]*IotNode

var MqttClient3 mqtt.Client
var MqttClient3b mqtt.Client
var MqttDown string

var SvcNode *IotNode

var Pomp *IotNode
var Dimmer *IotNode
var Ketel *IotNode
var P1 *IotNode
var CloseIn *IotNode
var Serial1 *IotNode
var Serial2 *IotNode
var Chint *IotNode
var Test *IotNode
var Afzuiging *IotNode

var Sonoff *IotNode
var Sonoff2 *IotNode

var Neo31 *IotNode
var Kerstboom *IotProp

var Son32 *IotNode
var Son32Knop *IotProp

var KamerTemp *IotProp
var KetelSchakelaar *IotProp
var PompTemp *IotProp

var Switch *IotProp

var LogLevel *IotProp

// for sync LogLevel to Connector
var ConnNode *IotNode

// var ConnLogLevel *IotProp

var TimerMode *IotProp

var DatabaseNew *sql.DB

// IotConn ...
type IotConn struct {
	Service        string
	Protocol       string
	Conn           net.Conn
	ConnectTimeout int
	ReadTimeout    int
	PingInterval   int
	Silent         bool
	Connected      bool
}

// var IDFactor int

var (
	Detail *log.Logger
	Trace  *log.Logger
	Info   *log.Logger
	Warn   *log.Logger
	Err    *log.Logger
)

var propToLog int
var nodeToLog int
var codeToLog string
var LogDebug bool

func init() {
	Nodes = make(map[int]*IotNode)
	MqttDown = "??"
}

func main() {

	// fmt.Printf("Config %s\n", Config())

	log.Printf("iot.main")

	payload := `{"Cmd":"timedataDiff","VarId":311,"Start":1573945200,"Stop":1574031600}`
	iotPayload := ToPayload(payload)

	// fmt.Printf("payload:%s\niotPayload:%+v\n\n", payload, iotPayload)

	// payload := "ping"
	// iotPayload := ToPayload(payload)

	// fmt.Printf("payload:%s\niotPayload:%+v\n\n", payload, iotPayload)

	// payload := "mvcdata,global"
	// iotPayload := ToPayload(payload)

	// fmt.Printf("payload:%s\niotPayload:%+v\n\n", payload, iotPayload)

	// payload = "U,1,1,1,1,1"
	// iotPayload = ToPayload(payload)

	// fmt.Printf("payload:%s\niotPayload:%+v\n\n", payload, iotPayload)

	// payload = "timedata,111"
	// iotPayload = ToPayload(payload)

	fmt.Printf("payload:%s\niotPayload:%+v\n\n", payload, iotPayload)

}

// LogInfo ...
func LogInfo() {
	Info.Printf("codeToLog:%s nodeToLog:%d propToLog:%d\n", codeToLog, nodeToLog, propToLog)
}

// LogProp ...
func LogProp(prop *IotProp) bool {

	if LogNode(prop.Node) &&
		(propToLog == 0 || prop.PropId == propToLog) {

		return true
	}
	return false
}

// LogPayload ...
func LogPayload(payload *IotPayload) bool {

	if codeToLog == "s" ||
		((payload.PropId == propToLog || propToLog == 0) &&
			(payload.NodeId == nodeToLog || nodeToLog == 0) &&
			(propToLog != 0 || nodeToLog != 0)) {

		return true

	} else {
		return false
	}
}

// LogNode ...
func LogNode(node *IotNode) bool {

	if node.NodeId == nodeToLog && nodeToLog != 0 {
		return true
	}
	return false
}

// LogCode ...
func LogCode(code string) bool {

	if code == codeToLog {
		return true
	}
	return false
}

// MvcUp  put on UpQueue {"mvcup":{"nx_py" || "py":[v,recent]}}
func MvcUp(prop *IotProp) {

	var buff bytes.Buffer

	if prop.GlobalMvc {
		topic := fmt.Sprintf(`node/%d/global`, prop.NodeId)
		PropMvc(&buff, prop, true)
		payload := fmt.Sprintf(`{"mvcup":{%s}}`, buff.String())

		if LogProp(prop) {
			Trace.Printf("mvcUp %s [%s]", payload, topic)
		}

		token := MqttClient3b.Publish(topic, byte(0), false, payload)
		if token.Wait() && token.Error() != nil {
			Err.Printf("error %s [%s]\n", payload, topic)
			Err.Printf("Fail to publish, %v", token.Error())
		}
	}

	if prop.GlobalMvc ||
		prop.LocalMvc {

		topic := fmt.Sprintf(`node/%d/local`, prop.NodeId)
		buff.Reset()
		PropMvc(&buff, prop, false)
		payload := fmt.Sprintf(`{"mvcup":{%s}}`, buff.String())

		if LogProp(prop) {
			Trace.Printf("mvcUp %s [%s]\n", payload, topic)
		}

		token := MqttClient3b.Publish(topic, byte(0), false, payload)
		if token.Wait() && token.Error() != nil {
			Err.Printf("error %s [%s]", payload, topic)
			Err.Printf("Fail to publish, %v", token.Error())
		}
	}
}

// NewMQClient ...
func NewMQClient(connection string, clientID string) mqtt.Client {

	var mqttClient mqtt.Client

	mqttURI, err := url.Parse(connection)
	if err != nil {
		Err.Printf("mqtt parse err:%s [%s]", err.Error(), connection)
		return mqttClient
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(mqttURI.String())
	opts.AutoReconnect = true
	opts.SetKeepAlive(time.Second * time.Duration(60))
	if clientID != "" {
		opts.SetClientID(clientID) // Multiple connections should use different clientID for each connection, or just leave it blank
	}

	// If lost connection, reconnect again
	opts.SetConnectionLostHandler(func(client mqtt.Client, e error) {
		//	logger.Warn(Err.Sprintf("mqtt conntion lost: %v", e))
		fmt.Sprintf("mqtt reconnect ?? : %v\n", e)
	})

	mqttClient = mqtt.NewClient(opts) // connect to broker

	token := mqttClient.Connect()
	if token.Wait() && token.Error() != nil {
		fmt.Sprintf("mqtt connect failed: %v\n", token.Error())
		// logger.Fatalf("Fail to connect broker, %v",token.Error())
	}

	return mqttClient

}

// token := mqttClient.Publish(topic, byte(0), false, payload)

// client := mqConnect("iotCmd", uri)
// client.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {

// func mqConnect(clientId string, uri *url.URL) mqtt.Client {
// 	opts := createClientOptions(clientId, uri)
// 	client := mqtt.NewClient(opts)
// 	token := client.Connect()
// 	for !token.WaitTimeout(3 * time.Second) {
// 	}
// 	if err := token.Error(); err != nil {
// 		log.Fatal(err)
// 	}
// 	return client
// }

// func createClientOptions(clientId string, uri *url.URL) *mqtt.ClientOptions {
// 	opts := mqtt.NewClientOptions()
// 	opts.AddBroker(fmt.Sprintf("tcp://%s", uri.Host))
// 	opts.SetUsername(uri.User.Username())
// 	password, _ := uri.User.Password()
// 	opts.SetPassword(password)
// 	opts.SetClientID(clientId)
// 	return opts
// }

// Send ...
func Send(conn *IotConn, request string) (string, error) {

	//TODO check how 0x00 is handled
	var err error

	if !conn.Connected {

		conn.Conn, err = net.DialTimeout(conn.Protocol, conn.Service, 3*time.Second)

		if err == nil {
			if !conn.Silent {
				fmt.Printf("%s connected\n", conn.Service)
			}
			conn.Connected = true

		} else {
			if !conn.Silent {
				Err.Printf("Error: %s\n", err.Error())
			}
			return "connErr", err
		}
	}

	if !conn.Silent {
		fmt.Printf("Send>:%s[%s]\n", request, conn.Service)
	}

	_, err = conn.Conn.Write([]byte(fmt.Sprintf("%s\n", request)))

	if err != nil {
		if !conn.Silent {
			fmt.Printf("Error:%s\n", err.Error())
		}
		conn.Connected = false
		return "SendError", err
	}

	var char byte
	c := bufio.NewReader(conn.Conn)
	buff := make([]byte, 1024)
	buffPointer := 0

	conn.Conn.SetReadDeadline(time.Now().Add(time.Duration(conn.ReadTimeout) * time.Second))

	for {

		char, err = c.ReadByte()

		if err != nil {

			if !conn.Silent {
				fmt.Printf("Error:%s\n", err.Error())
			}
			conn.Connected = false
			return "ReadError", err

		} else {

			if buffPointer >= len(buff) {
				if !conn.Silent {
					fmt.Printf("ReadBufferOverflow<:%s\n", string(buff[0:buffPointer]))
				}
				return "ReadBufferOverflow", errors.New("ReadBufferOverflow")

			} else if char == 0x0d || char == 0x0a {
				buff[buffPointer] = char
				if !conn.Silent {
					fmt.Printf("Response<:%s\n", string(buff[0:buffPointer]))
				}
				return string(buff[0:buffPointer]), err
			} else {
				if !conn.Silent {
					//	fmt.Printf("-%c", char)
				}
				buff[buffPointer] = char
				buffPointer++
			}
		}
	}
}

func Config() string {

	if runtime.GOOS == "windows" {
		// dir, _ := filepath.Abs(os.Args[0])
		return filepath.Dir(os.Args[0]) + `\config.json`
	} else {

		return os.Getenv("HOME") + "/.go/" + filepath.Base(os.Args[0]) + ".json"
	}
}

func timedata(iotPayload *IotPayload) {

	//fmt.Printf("iot.timedata:%v\n", iotPayload)

	if len(iotPayload.Parms) < 2 &&
		iotPayload.Id == "" {
		iotPayload.Error = fmt.Sprintf(`{"retcode":99,"message":"%s: id missing"}`, iotPayload.Cmd)
		return
	}

	if iotPayload.Start == 0 &&
		iotPayload.Stop == 0 {

		// from start of day until now
		now := time.Now()
		iotPayload.Stop = time.Now().Unix()
		iotPayload.Start = iotPayload.Stop - int64((now.Second() + now.Minute()*60 + now.Hour()*3600)) // start of the day

	} else if iotPayload.Stop == 0 {

		// stop missing so take 1 day =  86400 seconds

		if iotPayload.Start > 100000 {
			iotPayload.Stop = iotPayload.Start + 86400
			iotPayload.Length = 86400

		} else {
			// use start of the day until end of the day
			iotPayload.Start = time.Now().Unix() + iotPayload.Start
			iotPayload.Stop = iotPayload.Start + 86400
			iotPayload.Length = 86400
		}

	} else {

		if iotPayload.Start > 100000 && iotPayload.Stop < 100000 {

			iotPayload.Stop = iotPayload.Start + iotPayload.Stop
			iotPayload.Length = int(iotPayload.Stop)

		} else if iotPayload.Start == 0 && iotPayload.Stop > 100000 {
			iotPayload.Start = time.Now().Unix() - int64((time.Now().Second() + time.Now().Minute()*60 + time.Now().Hour()*3600)) // start of the day

		} else if iotPayload.Start < 100000 && iotPayload.Stop > 100000 {
			iotPayload.Start = time.Now().Unix() + iotPayload.Start

		} else if iotPayload.Start == 0 && iotPayload.Stop < 100000 {

			iotPayload.Start = time.Now().Unix() - int64((time.Now().Second() + time.Now().Minute()*60 + time.Now().Hour()*3600)) // start of the day
			iotPayload.Stop = iotPayload.Start + iotPayload.Stop
			iotPayload.Length = int(iotPayload.Stop)

		} else if iotPayload.Start < 100000 && iotPayload.Stop < 100000 {

			iotPayload.Start = time.Now().Unix() + iotPayload.Start
			iotPayload.Stop = iotPayload.Start + iotPayload.Stop
			iotPayload.Length = int(iotPayload.Stop)
		}
	}
}

// ToPayload ...
func ToPayload(payload string) IotPayload {

	var myPayload IotPayload

	if strings.Contains(payload, ":") {

		/*
		 * deal with json comming in
		 */
		errrr := json.Unmarshal([]byte(payload), &myPayload)

		if errrr != nil {
			Err.Println("ToPayload json err:", errrr.Error())
			return myPayload
		}
		myPayload.IsJson = true

	} else if strings.Contains(payload, "{") {

		msgStart := strings.Index(payload, "{")
		msgEnd := strings.Index(payload, "}")

		if msgStart > -1 && msgEnd > -1 {
			payload = payload[msgStart+1 : msgEnd]

		} else if msgEnd > -1 {
			payload = payload[0:msgEnd]

		} else if msgStart > -1 {
			payload = payload[msgStart+1:]
		}
	}

	var parms []string

	if !myPayload.IsJson {

		if strings.Contains(payload, " ") {
			parms = strings.Split(payload, " ")
		} else if strings.Contains(payload, ",") {
			parms = strings.Split(payload, ",")
		} else if strings.Contains(payload, "&") {
			parms = strings.Split(payload, "&")
		} else if strings.Contains(payload, ";") {
			parms = strings.Split(payload, ";")
		} else {
			// parms = [1]string
			// parms[0] = payload
			// parms[0] = payload
			// fmt.Printf("Err iotSvc: %s \n", payload)
		}

		if len(parms) > 1 {
			myPayload.Cmd = parms[0]
		} else {
			myPayload.Cmd = payload
		}
	}

	myPayload.Parms = parms
	//fmt.Printf("ToPayload.parms:%q len:%d\n", parms, len(parms))

	if len(parms) > 1 &&
		parms[1][0:1] == "n" {
		myPayload.Id = parms[1]

	}

	//fmt.Printf("ToPayload.cmd %v\n", myPayload)

	if myPayload.Id != "" {
		startProp := strings.Index(myPayload.Id, "p")
		if startProp > 1 {
			myPayload.NodeId = aToI(myPayload.Id[1:startProp])
			myPayload.PropId = aToI(myPayload.Id[startProp+1:])
		}
	}

	switch myPayload.Cmd {

	case "u", "e", "r", "s":

		if len(parms) > 1 {
			myPayload.VarId = aToI(parms[1])
		}
		if len(parms) > 2 {
			myPayload.ConnId = aToI(parms[2])
		}
		if len(parms) > 3 {
			// myPayload.Val = aToI64(parms[3])
			myPayload.Val = parms[3]
		}
		if len(parms) > 4 {
			myPayload.Timestamp = aToI64(parms[4])
		}

		myPayload.Cmd = strings.ToUpper(myPayload.Cmd)
		// 	rsp.NodeId = nodeId(rsp.VarId)
		// 	rsp.PropId = nodeId(rsp.VarId)

	case "C":
		if len(parms) > 1 {
			myPayload.Cmd = parms[1]
		}
		if len(parms) > 2 {
			myPayload.NodeId = aToI(parms[2])
		}

		// ????
		// varId=IoTUtil.propKey(nodeId, sensorId);
		// changed = true;

	case "U", "E", "R", "S", "P", "O", "D":
		if len(parms) > 1 {
			myPayload.NodeId = aToI(parms[1])
		}
		if len(parms) > 2 {
			myPayload.PropId = aToI(parms[2])
		}
		if len(parms) > 3 {
			myPayload.ConnId = aToI(parms[3])
		}
		if len(parms) > 4 {
			myPayload.Val = parms[4]
		}
		if len(parms) > 5 {
			myPayload.Timestamp = aToI64(parms[5])
		}

		// myPayload.VarId = myPayload.NodeId*IDFactor + myPayload.PropId

	case "set":
		myPayload.Cmd = "S"

		if len(parms) == 3 {
			myPayload.Val = parms[2]

		}

		// myPayload.VarId = myPayload.NodeId*IDFactor + myPayload.PropId

	case "setVal":
		myPayload.Cmd = "S"
		if len(parms) > 1 {
			myPayload.NodeId = aToI(parms[1])
		}
		if len(parms) > 2 {
			myPayload.PropId = aToI(parms[2])
		}
		if len(parms) > 3 {
			myPayload.Val = parms[3]
		}

		// myPayload.VarId = myPayload.NodeId*IDFactor + myPayload.PropId

	case "ota":
		myPayload.Cmd = "O"
		myPayload.NodeId = aToI(parms[1])

	// case "P", "p", "R", "r", "O", "o":
	// 	myPayload.Cmd = myPayload.Cmd
	// 	myPayload.NodeId = aToI(parms[1])

	case "sendN":
		myPayload.Cmd = "N"
		myPayload.NodeId = aToI(parms[1])

	case "hang":
		myPayload.Cmd = "B"
		myPayload.NodeId = aToI(parms[1])

	case "test":
		if len(parms) > 1 {
			myPayload.Cmd = "T"
			myPayload.NodeId = aToI(parms[1])
		}

	case "rstBcount":
		myPayload.Cmd = "b"
		myPayload.NodeId = aToI(parms[1])

	default:

		if len(parms) == 0 &&
			strings.Contains(payload, "/") &&
			strings.HasPrefix(payload, "iot") {
			myPayload.Cmd = "subscribe"
			myPayload.Val = payload
		}

		if len(parms) == 2 {
			myPayload.Val = parms[1]
		}

		if len(parms) == 3 {
			i, err := strconv.Atoi(parms[1])
			if err == nil {
				myPayload.PropId = i
				myPayload.Val = parms[2]
			} else {

			}
		}
		if len(parms) == 4 {
			i, err := strconv.Atoi(parms[1])
			if err == nil {
				myPayload.NodeId = i
				i, err = strconv.Atoi(parms[1])
				if err == nil {
					myPayload.PropId = i
					myPayload.Val = parms[3]
				}
			}
			// myPayload.NodeId = aToI(parms[1])
			// myPayload.PropId = aToI(parms[2])
		}

	}

	if myPayload.Cmd == "u" || myPayload.Cmd == "e" || myPayload.Cmd == "U" || myPayload.Cmd == "E" || myPayload.Cmd == "D" {

		if myPayload.ConnId == 0 {
			myPayload.ConnId = myPayload.NodeId
		}
		// 	// 	changed = calcTimeStampInSec(timestamp);
	}

	if strings.HasPrefix(myPayload.Cmd, "time") {

		if len(parms) > 1 {
			myPayload.Id = parms[1]
		}
		if len(parms) > 2 {
			myPayload.Start = aToI64(parms[2])
		}
		if len(parms) > 3 {
			myPayload.Stop = aToI64(parms[3])
		}
		timedata(&myPayload)

	} else if myPayload.Cmd == "mvcdata" {
		if len(myPayload.Parms) > 2 {
			myPayload.NodeId = aToI(parms[2])
		}

	}

	if myPayload.Cmd == "S" {
		myPayload.Cmd = "set"
	}

	return myPayload
}

func aToI(a string) int {

	//fmt.Printf("aToI:%s\n", a)
	i, err := strconv.Atoi(a)
	checkError(err)
	return i
}

func aToI64(a string) int64 {
	//fmt.Printf("aToI64:%s\n", a)
	i64, err := strconv.ParseInt(a, 10, 32)
	checkError(err)
	return i64
}

func checkError(err error) {
	if err != nil {
		Err.Printf("err: " + err.Error())
		fmt.Fprintf(os.Stderr, "iot Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func FmtHHMM(d time.Duration) string {
	d = d.Round(time.Minute)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	return fmt.Sprintf("%02d:%02d", h, m)
}
func FmtMMSS(d time.Duration) string {
	d = d.Round(time.Second)
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	return fmt.Sprintf("%02d:%02d", m, s)
}

func SqlFloat32(localVar *float32, column sql.NullFloat64) {
	if column.Valid {
		*localVar = float32(column.Float64)
	}
}

func SqlBoolInt(localVar *bool, column sql.NullInt64) {
	if column.Valid {
		*localVar = column.Int64 > 0
	}
}

func SqlInt(localVar *int, column sql.NullInt64) {
	if column.Valid {
		*localVar = int(column.Int64)
	}
}

func SqlInt64(localVar *int64, column sql.NullInt64) {
	if column.Valid {
		*localVar = column.Int64
	}
}

func SqlString(localVar *string, column sql.NullString) {
	if column.Valid {
		*localVar = column.String
	}
}

// NewEchoService ...
func NewEchoService(port string) error {

	var err error
	var tcpAddr *net.TCPAddr

	service := ":" + port

	tcpAddr, err = net.ResolveTCPAddr("tcp", service)
	if err != nil {
		return err
	}

	tcpListener, errr := net.ListenTCP("tcp", tcpAddr)
	if errr != nil {
		return errr
	}

	fmt.Printf("NewEchoService: " + tcpAddr.IP.String() + "\r\n")

	go echoListener(tcpListener)

	return err
}

func echoListener(listener *net.TCPListener) {

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		// multi threading:
		go echoHandler(conn)
	}
}

func echoHandler(conn net.Conn) {

	//TODO prevent long request attack
	//TODO check how 0x00 is being handled

	fmt.Printf("handleEchoClient \r\n")

	conn.SetReadDeadline(time.Now().Add(49 * time.Second))

	defer conn.Close() // close connection before exit

	var err error
	var char byte

	buff := make([]byte, 512)
	pntr := 0
	c := bufio.NewReader(conn)

	for {
		// read a single byte which contains the message length
		char, err = c.ReadByte()

		//fmt.Printf("ReadByte %x\n", buff )

		if err != nil {
			Err.Println(err)
			break
		} else if char == 0x0d || char == 0x0a || pntr >= 64 {

			buff[pntr] = 0x00

			if pntr > 0 {
				buff[pntr] = 0x0d
				pntr++
				buff[pntr] = 0x00
				conn.Write(buff[0:pntr])
			}

			pntr = 0
		} else {
			buff[pntr] = char
			pntr++
		}

		if err != nil {
			Err.Println(err)
			break
		}
	}
}

//  TODO NewForwardService ------------------------------------
func startForwardService(port string, target net.Conn) error {

	forwardListener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return err
	}

	fmt.Printf("forwardListener :" + port + "\r")

	go serviceForwardListener(forwardListener, target)

	return err
}

func serviceForwardListener(listener net.Listener, target net.Conn) {

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		// multi threading:
		go handleForwardClient(conn, target)
	}
}

func handleForwardClient(conn net.Conn, target net.Conn) {

	fmt.Printf("handleForwardClient\r")

	go forward("fwdUp", conn, target)

	go forward("fwdDown", target, conn)
}

func forward(id string, source net.Conn, target net.Conn) {

	var char byte
	var err error

	defer source.Close() // close connection before exit

	c := bufio.NewReader(source)
	t := bufio.NewWriter(target)

	timeoutDuration := 3 * time.Second

	for {

		//source.SetReadDeadline(time.Now().Add(timeoutDuration))
		char, err = c.ReadByte()

		if err == nil {

			//fmt.Printf("%s < %x-%c\r", id, char, char )

			err = t.WriteByte(char)

			if err != nil {
				Err.Printf("forward write err %s\n", err.Error())
				return
			}

			if char == 0x0d || char == 0x0a || c.Buffered() >= c.Size() {

				if c.Buffered() >= c.Size() {
					Err.Printf("%s flush %d\n", id, t.Buffered())
				}

				target.SetWriteDeadline(time.Now().Add(timeoutDuration))
				t.Flush()

				// calc new deadline

			}

		} else {

			Err.Printf("forward read err %s\n", err.Error())
			return
		}
	}
}

func InitLogging(
	traceHandle io.Writer,
	infoHandle io.Writer,
	warningHandle io.Writer,
	errorHandle io.Writer) {

	Detail = log.New(infoHandle,
		"",
		0)

	Trace = log.New(traceHandle,
		"_",
		log.Ltime)

	Info = log.New(infoHandle,
		"",
		log.Ltime)

	Warn = log.New(warningHandle,
		"w)",
		log.Ldate|log.Ltime|log.Lshortfile)

	Err = log.New(errorHandle,
		"e)",
		log.Ldate|log.Ltime|log.Lshortfile)

}

// func AppendByte(slice []byte, data ...byte) []byte {
// 	m := len(slice)
// 	n := m + len(data)
// 	if n > cap(slice) { // if necessary, reallocate
// 		// allocate double what's needed, for future growth.
// 		newSlice := make([]byte, (n+1)*2)
// 		copy(newSlice, slice)
// 		slice = newSlice
// 	}
// 	slice = slice[0:n]
// 	copy(slice[m:n], data)
// 	return slice
// }

// t := make([]byte, len(s), (cap(s)+1)*2)
// copy(t, s)
// s = t
