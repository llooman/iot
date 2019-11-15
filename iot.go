// package main

package iot

/*
go get -u all
go get -u github.com/godoctor/godoctor

go get github.com/tkanos/gonfig
*/

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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

type TimeDataRequest struct {
	Cmd    string
	Id     int64
	Start  int64
	Stop   int64
	Length int64
}

type IotPayload struct {
	Cmd       string
	Parms     []string
	NodeId    int
	PropId    int
	VarId     int
	ConnId    int
	Val       int64
	Timestamp int64
	Start     int64
	Stop      int64
	Length    int // ??
	Error     string
	IsJson    bool
}

type CmdRequest struct {
	Cmd   string
	Parms []string
}

type IotMsg struct {
	Cmd       string
	NodeId    int
	PropId    int
	VarId     int
	ConnId    int
	Val       int
	Timestamp int
}

type IotNode struct {
	NodeId    int
	ConnId    int
	Name      string
	DefDirty  bool
	Dirty     bool
	FreeMem   int
	BootCount int
	BootStamp int64
	Timestamp int64
}

// IotProp ...
type IotProp struct {
	Name   string
	VarId  int
	NodeId int
	PropId int
	Node   *IotNode

	FromDb   bool
	DefDirty bool
	MemDirty bool
	IsNew    bool

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
	Err      int64 // &
	ErrStamp int64
	InErr    bool // -

	DrawType   int
	DrawColor  string
	DrawOffset int
	DrawFactor float32

	RefreshRate int
	RetryCount  int
	NextRefresh int64

	// Modified int64 // timestamp
	// PropType int   // parm, iotVal, log
	// Valid    int
}

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

var IDFactor int

func init() {

	IDFactor = 1000
}

func main() {

	// fmt.Printf("Config %s\n", Config())

	payload := "setVal,4,52,30"
	iotPayload := ToPayload(payload)

	// payload := "{ota,4}"
	// iotPayload := ToPayload(payload)

	// fmt.Printf("payload:%s\niotPayload:%+v\n\n", payload, iotPayload)

	// payload := "mvcdata&node&3"
	// iotPayload := ToPayload(payload)

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

// NewMQClient ...
func NewMQClient(connection string, clientID string) mqtt.Client {

	var mqttClient mqtt.Client

	mqttURI, err := url.Parse(connection)
	if err != nil {
		fmt.Printf("mqtt parse err:%s [%s]", err.Error(), connection)
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
		//	logger.Warn(fmt.Sprintf("mqtt conntion lost: %v", e))
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
				fmt.Printf("Error: %s\n", err.Error())
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

	// fmt.Printf("timedata:%+v", iotPayload)

	if len(iotPayload.Parms) < 2 {
		iotPayload.Error = fmt.Sprintf(`{"retcode":99,"message":"%s: id missing"}`, iotPayload.Cmd)
		return
	}

	iotPayload.VarId, _ = strconv.Atoi(iotPayload.Parms[1])
	iotPayload.PropId = iotPayload.VarId % IDFactor
	iotPayload.NodeId = iotPayload.VarId / IDFactor

	if len(iotPayload.Parms) == 2 { // from start of day until now

		now := time.Now()
		iotPayload.Stop = now.Unix()
		iotPayload.Start = iotPayload.Stop - int64((now.Second() + now.Minute()*60 + now.Hour()*3600)) // start of the day

	}
	if len(iotPayload.Parms) == 3 { // stop missing so take 1 day =  86400 seconds
		iotPayload.VarId, _ = strconv.Atoi(iotPayload.Parms[1])
		parm3, _ := strconv.Atoi(iotPayload.Parms[2])
		now := time.Now()

		fmt.Printf("timedata.parm3:%d\n", parm3)

		if parm3 > 100000 {
			iotPayload.Start = int64(parm3)
			iotPayload.Stop = iotPayload.Start + 86400
			iotPayload.Length = 86400

			// } else if start == 0 {
			// 	req.Start  = int64(start)
			// 	req.Stop = req.Start + int64(stop)

		} else { // use start of the day until end of the day
			// dayStart := now.Unix() -  int64((now.Second()+now.Minute()*60+now.Hour()*3600))
			iotPayload.Start = now.Unix() + int64(parm3)
			iotPayload.Stop = iotPayload.Start + 86400
			iotPayload.Length = 86400
		}
	}
	if len(iotPayload.Parms) == 4 {

		iotPayload.VarId, _ = strconv.Atoi(iotPayload.Parms[1])

		parm3, _ := strconv.Atoi(iotPayload.Parms[2])
		parm4, _ := strconv.Atoi(iotPayload.Parms[3])
		now := time.Now()

		if parm3 > 100000 && parm4 > 100000 {
			iotPayload.Start = int64(parm3)
			iotPayload.Stop = int64(parm4)

		} else if parm3 > 100000 && parm4 < 100000 {
			iotPayload.Start = int64(parm3)
			iotPayload.Stop = iotPayload.Start + int64(parm4)
			iotPayload.Length = parm4 //86400

		} else if parm3 == 0 && parm4 > 100000 {
			iotPayload.Start = now.Unix() - int64((now.Second() + now.Minute()*60 + now.Hour()*3600)) // start of the day
			iotPayload.Stop = int64(parm4)

		} else if parm3 < 100000 && parm4 > 100000 {
			iotPayload.Start = now.Unix() + int64(parm3)
			iotPayload.Stop = int64(parm4)

		} else if parm3 == 0 && parm4 < 100000 {
			iotPayload.Start = now.Unix() - int64((now.Second() + now.Minute()*60 + now.Hour()*3600)) // start of the day
			iotPayload.Stop = iotPayload.Start + int64(parm4)
			iotPayload.Length = parm4 //86400

		} else if parm3 < 100000 && parm4 < 100000 {

			iotPayload.Start = now.Unix() + int64(parm3)
			iotPayload.Stop = iotPayload.Start + int64(parm4)
			iotPayload.Length = parm4 //86400
		}
	}

	// return ""
	//	fmt.Printf(`timedata: {"id":%d,"start":%d,"stop":%d }\n\n`, req.Id, req.Start , req.Stop )

}

// ToPayload ...
func ToPayload(payload string) IotPayload {

	var myPayload IotPayload

	if strings.Contains(payload, ":") {
		// deal with json comming in
		var req TimeDataRequest

		errrr := json.Unmarshal([]byte(payload), &req)

		if errrr != nil {
			fmt.Println("ToIotPayload json err:", errrr.Error())
			return myPayload
		}

		myPayload.Cmd = req.Cmd
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

	if strings.Contains(payload, " ") {
		parms = strings.Split(payload, " ")
	} else if strings.Contains(payload, ",") {
		parms = strings.Split(payload, ",")
	} else if strings.Contains(payload, "&") {
		parms = strings.Split(payload, "&")
	} else if strings.Contains(payload, ";") {
		parms = strings.Split(payload, ";")
	} else {
		fmt.Printf("Err iotSvc: %s \n", payload)
	}

	myPayload.Parms = parms
	// fmt.Printf("ToIotPayload.parms:%q len:%d\n", parms, len(parms))

	if len(parms) < 1 {
		myPayload.Cmd = payload

	} else {
		myPayload.Cmd = parms[0]

	}

	// 	if(   Character.isDigit(cmd.charAt(0))
	// 	||	( Character.isDigit(cmd.charAt(1)) && cmd.charAt(0) == 'n' )
	//  ){
	// 		logger.info("domo.command: "+cmd+" "+parm1+" "+parm2+" "+parm3+" "+parm4);
	// 	 if(cmd.charAt(0) == 'n') cmd = cmd.substring(1);
	// 	 parm4 = parm3;
	// 	 parm3 = parm2;
	// 	 parm2 = parm1.replace("_", "");
	// 	 parm1 = cmd;
	// 	 cmd = "node";
	// 	}

	// case "setVal":;
	// if(parm1.indexOf("_")>0)
	// {
	// 	String parms[] = parm1.split("_") ;
	// 	node = getNode(parms[0]);
	// 	response = node.command(cmd, parms[1], parm2);
	// }
	// else
	// {
	// 	node = getNode(parm1);
	// 	response = node.command(cmd, parm2, parm3);
	// }
	// break;

	switch myPayload.Cmd {

	case "u", "e", "r", "s":

		if len(parms) > 1 {
			myPayload.VarId = aToI(parms[1])
		}
		if len(parms) > 2 {
			myPayload.ConnId = aToI(parms[2])
		}
		if len(parms) > 3 {
			myPayload.Val = aToI64(parms[3])
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

	case "U", "E", "R", "S":
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
			myPayload.Val = aToI64(parms[4])
		}
		if len(parms) > 5 {
			myPayload.Timestamp = aToI64(parms[5])
		}

		myPayload.VarId = myPayload.NodeId*IDFactor + myPayload.PropId

	case "setVal", "setProp":
		myPayload.Cmd = "S"
		if len(parms) > 1 {
			myPayload.NodeId = aToI(parms[1])
		}
		if len(parms) > 2 {
			myPayload.PropId = aToI(parms[2])
		}
		if len(parms) > 3 {
			myPayload.Val = aToI64(parms[3])
		}
		// if len(parms) > 4 {
		// 	myPayload.Val = aToI64(parms[4])
		// }
		// if len(parms) > 5 {
		// 	myPayload.Timestamp = aToI64(parms[5])
		// }

		myPayload.VarId = myPayload.NodeId*IDFactor + myPayload.PropId

	case "ota":
		myPayload.Cmd = "O"
		myPayload.NodeId = aToI(parms[1])

	case "sendN":
		myPayload.Cmd = "N"
		myPayload.NodeId = aToI(parms[1])

	case "hang":
		myPayload.Cmd = "B"
		myPayload.NodeId = aToI(parms[1])

	case "test":
		myPayload.Cmd = "T"
		myPayload.NodeId = aToI(parms[1])

	case "rstBcount":
		myPayload.Cmd = "b"
		myPayload.NodeId = aToI(parms[1])
	}

	if myPayload.Cmd == "u" || myPayload.Cmd == "e" || myPayload.Cmd == "U" || myPayload.Cmd == "E" {

		if myPayload.ConnId == 0 {
			myPayload.ConnId = myPayload.NodeId
		}
		// 	// 	changed = calcTimeStampInSec(timestamp);
	}

	if strings.HasPrefix(myPayload.Cmd, "time") {
		timedata(&myPayload)

	} else if myPayload.Cmd == "mvcdata" {
		if len(myPayload.Parms) > 2 {
			myPayload.NodeId = aToI(parms[2])
		}

	}

	return myPayload
}

func aToI(a string) int {

	i, err := strconv.Atoi(a)
	// i64, err := strconv.ParseInt(s, 10, 32)
	checkError(err)
	return i
}

func aToI64(a string) int64 {

	// i, err := strconv.Atoi(a)
	i64, err := strconv.ParseInt(a, 10, 32)
	checkError(err)
	return i64
}

func checkError(err error) {
	if err != nil {
		fmt.Printf("err: " + err.Error())
		fmt.Fprintf(os.Stderr, "iot Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func nodeId(varID int) int {

	if varID < IDFactor {
		// fmt.Printf("Warn util.nodeId: %s util.nodeId:  Id "+varId+" <IDFactor!"\n", payload)
		//logger.error("util.nodeId:  Id "+varId+" <IDFactor!");
		return varID
	}
	return varID / IDFactor
}

func propId(varID int) int {

	return varID % IDFactor
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
			fmt.Println(err)
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
			fmt.Println(err)
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
				fmt.Printf("forward write err %s\n", err.Error())
				return
			}

			if char == 0x0d || char == 0x0a || c.Buffered() >= c.Size() {

				if c.Buffered() >= c.Size() {
					fmt.Printf("%s flush %d\n", id, t.Buffered())
				}

				target.SetWriteDeadline(time.Now().Add(timeoutDuration))
				t.Flush()

				// calc new deadline

			}

		} else {

			fmt.Printf("forward read err %s\n", err.Error())
			return
		}
	}
}
