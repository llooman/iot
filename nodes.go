package iot

import (
	"bytes"
	"fmt"

	"log"
	"strconv"
	"strings"
	"time"
)

// OnNodePropRead ...
func OnNodePropRead(prop *IotProp) {

	if prop.Node.NodeId == 2 &&
		prop.PropId == 10 {

		OnChangeLogOptions(prop.ValString)
	}
}

// OnNodePropSave ...
func OnNodePropSave(prop *IotProp) {

	if prop.Node.NodeId == 2 &&
		prop.PropId == 10 {

		OnChangeLogOptions(prop.ValString)

		// update connector
		connProp := GetProp(1, 10)
		if connProp != nil {
			connProp.ValString = prop.ValString
			SendProp(connProp, "S", prop.ValString)
		}
	}
}

func OnChangeLogOptions(newVal string) {

	parms := strings.Split(newVal, ".")
	if len(parms) > 1 {

		_, nodeToLog, codeToLog = testParmInt(parms[0])
		_, propToLog, codeToLog = testParmInt(parms[1])

	} else {

		_, nodeToLog, codeToLog = testParmInt(newVal)
		propToLog = 0

		// f, err := strconv.Atoi(prop.ValString)

		// if err != nil {
		// 	codeToLog = prop.ValString
		// 	propToLog = 0
		// 	nodeToLog = 0
		// } else {
		// 	codeToLog = ""
		// 	nodeToLog = f
		// 	propToLog = 0

		// }
	}

	Info.Printf("codeToLog:%s nodeToLog:%d propToLog:%d\n", codeToLog, nodeToLog, propToLog)

}

func testParmInt(parm string) (bool, int, string) {

	i, err := strconv.Atoi(parm)

	if err != nil {
		return false, 0, parm

	} else {
		return true, i, ""
	}
}

// CheckConnId ...
func CheckConnId(node *IotNode, newConnId int) {

	if node.ConnId != newConnId &&
		newConnId > 0 {

		node.ConnId = newConnId
		// node.IsDefSaved = false
		PersistNodeDef(node)

		Trace.Printf("updateConnId node:%d to %d\n", node.NodeId, newConnId)
	}

	if node.IsOffline {
		for _, prop := range node.Props {
			if prop.RefreshRate > 0 &&
				prop.NextRefresh > time.Now().Unix()+3 {

				prop.NextRefresh = time.Now().Unix()
				PersistPropToMem(prop)
			}
		}

		node.IsOffline = false
	}

}

// SendNodeCmd ...
func SendNodeCmd(cmd string, nodeId int, newVal string) string {

	node := GetNode(nodeId)

	if newVal == "" {
		newVal = "0"
	}

	topic := MqttDown + "/" + strconv.Itoa(node.ConnId)
	payload := fmt.Sprintf("{%s,%d,0,%d,%s}", cmd, nodeId, node.ConnId, newVal)

	if LogNode(node) {
		Info.Printf("SendNodeCmd %s [%s]\n", payload, topic) // {S,4,52,1,55} [iotOut/11]
	}

	token := MqttClient3b.Publish(topic, byte(0), false, payload)
	if token.Wait() && token.Error() != nil {
		Err.Printf("error %s [%s]\n", payload, MqttDown)
		Err.Printf("Fail to publish, %v", token.Error())
		return "SendNodeCmd3b err:"
	}

	token = MqttClient3.Publish(topic, byte(0), false, payload)
	if token.Wait() && token.Error() != nil {
		Err.Printf("error %s [%s]\n", payload, MqttDown)
		Err.Printf("Fail to publish, %v", token.Error())
		return "SendNodeCmd3 err:"
	}

	return ""
}

// PersistNodeDef ...
func PersistNodeDef(node *IotNode) {

	persist, err := DatabaseNew.Prepare(`
	INSERT INTO nodesDef( nodeId, 
		name, 
		connId,
		nodeType ) VALUES(?, ?, ?, ? ) 
	ON DUPLICATE KEY UPDATE name=?, 
		connId=?,
		nodeType=? `)
	if err != nil {
		Err.Printf("persistNodeDef: %v", err.Error())
	}

	defer persist.Close()

	_, err = persist.Exec(
		node.NodeId,
		node.Name,
		node.ConnId,
		node.NodeType,
		node.Name,
		node.ConnId,
		node.NodeType)

	if err != nil {
		Err.Printf("persistNodeDef: %v", err.Error())
	}
	node.IsDefSaved = true
}

func PersistNodeMem(node *IotNode) {

	persist, err := DatabaseNew.Prepare(`
	INSERT INTO nodesMem( nodeId, 
		bootCount, 
		freeMem,
		bootTime,
		timestamp ) VALUES(?, ?, ?, ?, ? ) 
	ON DUPLICATE KEY UPDATE bootCount=?, 
		freeMem=?,
		bootTime=?,
		timestamp=? `)
	if err != nil {
		Err.Printf("persistNodeMem: %v", err.Error())
	}

	defer persist.Close()

	_, err = persist.Exec(
		node.NodeId,
		node.BootCount,
		node.FreeMem,
		node.BootStamp,
		node.Timestamp,
		node.BootCount,
		node.FreeMem,
		node.BootStamp,
		node.Timestamp)

	if err != nil {
		Err.Printf("persistNodeMem: %v", err.Error())
	}
	node.IsDataSaved = true

}

func mcvIp(prop *IotProp) string {

	int64IP := prop.Val
	ip4 := int64IP % 256
	int64IP = int64IP / 256
	ip3 := int64IP % 256
	int64IP = int64IP / 256
	ip2 := int64IP % 256
	ip1 := int64IP / 256

	return fmt.Sprintf(`,"ip":["%d.%d.%d.%d",%d]`, ip4, ip3, ip2, ip1, PropRecent(prop))
	//  fmt.Sprintf(`,"iphref":["http://%03d.%03d.%03d.%03d/up"]`, ip4, ip3, ip2, ip1))
}

func NodeMvc(writer *bytes.Buffer, node *IotNode, global bool) {

	// ,"up":{"v":"166:43"},"last":{"v":"0:0:3"},"active":{"v":1},
	// 		socketOut.print (",\"active\":{\"v\":"+((active)?1:0)+"}");

	Trace.Printf("mvcDataNode.nodeId:%d\n", node.NodeId)
	writer.Write([]byte(fmt.Sprintf(`{"mvcdata3":{"id":["%d"]`, node.NodeId)))

	writer.Write([]byte(fmt.Sprintf(`,"caption":["%s"]`, node.Name)))
	writer.Write([]byte(fmt.Sprintf(`,"html":["%s"]`, node.Name)))

	if node.Timestamp > 0 {
		// diff := now().Sub(node.Timestamp)
		// duration := time.Now().Sub(time.Unix(node.Timestamp, 0))
		// writer.Write([]byte(fmt.Sprintf(`,"last":["%s"]`, FmtMMSS(duration))))
	}

	for _, prop := range node.Props {
		//fmt.Println("mvcDataGlobal varID:", varID, "prop:", prop)
		PropMvc(writer, prop, global)
	}

	if node.NodeType == 1 {
		ip := GetNodeProp(node, 9)
		writer.Write([]byte(mcvIp(ip)))

	} else if node.NodeId == 7 {

		ip := GetNodeProp(node, 9)
		writer.Write([]byte(mcvIp(ip)))

	} else if node.NodeId == 9 {
		PropMvc(writer, KamerTemp, true)
		PropMvc(writer, KetelSchakelaar, true)
		PropMvc(writer, PompTemp, true)

	}

	writer.Write([]byte(fmt.Sprintf("}}\n")))
}

// GetNode ...
func GetNode(nodeID int) *IotNode {

	//fmt.Printf("getNode %d\n", nodeID)

	if node, found := Nodes[nodeID]; found {
		return node
	}

	var node = &IotNode{
		NodeId: nodeID,
		Name:   "node" + strconv.Itoa(nodeID)}

	node.Props = make(map[int]*IotProp)
	Nodes[nodeID] = node

	if nodeID >= 30 &&
		node.NodeType != 1 {
		node.NodeType = 1

	}
	return node
}

// GetOrDefaultNode ...
func GetOrDefaultNode(nodeID int, name string) *IotNode {

	// Info.Printf("GetOrDefaultNode %d %s", nodeID, name)

	node := GetNode(nodeID)

	if !node.IsInit {
		// fmt.Printf("GetOrDefaultNode.Init: n%d", nodeID)
		node.Name = name
		node.NodeType = 0
		node.IsInit = true
		PersistNodeDef(node)
	}

	// if nodeID >= 30 &&
	// 	node.NodeType != 1 {
	// 	node.NodeType = 1
	// 	PersistNodeDef(node)
	// }

	GetOrDefaultProp(node, 1, "boot", 0, 3600, 0, 0, false)
	GetOrDefaultProp(node, 5, "ping", 0, 60, 0, 0, false)

	if node.NodeType == 1 {

		GetOrDefaultProp(node, 9, "ip", 0, 3600, 0, 0, false)
		GetOrDefaultProp(node, 10, "button", 0, 3600, 0, 0, false) // button 	read
		GetOrDefaultProp(node, 12, "relais", 0, 60, 0, 0, true)    // relais   	read/write
		GetOrDefaultProp(node, 13, "led", 0, 60, 0, 0, false)      // led   		read/write
		GetOrDefaultProp(node, 14, "ota", 0, 60, 0, 0, false)      // led   		read/write

		//fmt.Printf("GetOrDefaultProp n%dp14/ota\n", node.NodeId)

	} else if node.NodeId == 1 {
		node.ConnId = 1
		n1p10 := GetOrDefaultProp(node, 10, "loglevel", 0, 0, 0, 0, true)
		n1p10.IsStr = true

	} else if node.NodeId == 2 {
		n0p10 := GetOrDefaultProp(node, 10, "loglevel", 0, 0, 0, 0, true)
		n0p10.IsStr = true
		// GetOrDefaultProp(node, 20, "thuis", 0, 3600, 0, 0, true)
		// GetOrDefaultProp(node, 21, "vakantie", 0, 3600, 0, 0, true)

		// heapMem 	= getSensor(12, "heapMem");
		// linuxMem 	= getSensor(13, "linuxMem");

		GetOrDefaultProp(node, 30, "timerMode", 0, 0, 0, 0, true) // seconds or minutes

	} else if node.NodeId == 3 {

		GetOrDefaultProp(node, 13, "pomp", 0, 60, 1, 0, true)
		GetOrDefaultTemp(node, 20, "aanvoer", 2, true)
		GetOrDefaultTemp(node, 11, "retour", 2, true)
		GetOrDefaultTemp(node, 12, "ruimte", 2, true)
		GetOrDefaultTemp(node, 14, "muurin", 2, true)
		GetOrDefaultTemp(node, 15, "afvoer", 2, true)
		GetOrDefaultTemp(node, 16, "keuken", 2, true)

		GetOrDefaultProp(node, 52, "refTemp", 1, 3600, 0, 0, true)
		GetOrDefaultProp(node, 53, "deltaTemp", 1, 3600, 0, 0, true)
		GetOrDefaultProp(node, 54, "duty", 0, 3600, 0, 0, true)
		GetOrDefaultProp(node, 55, "dutymin", 0, 3600, 0, 0, true)

		GetOrDefaultProp(node, 70, "testStr", -1, 3600, 0, 0, true)

	} else if node.NodeId == 4 {
		GetOrDefaultProp(node, 52, "dimmer", 0, 60, 0, 0, true)
		GetOrDefaultProp(node, 53, "defLevel", 0, 3600, 0, 0, true)
		GetOrDefaultProp(node, 11, "vin", 1, 60, 0, 0, true)

	} else if node.NodeId == 5 {
		GetOrDefaultProp(node, 52, "thermostaat", 0, 60, 1, 0, true)
		GetOrDefaultProp(node, 53, "thermostaatPeriode", 0, 3600, 0, 0, true)
		GetOrDefaultTemp(node, 41, "CVTemp", 2, true)
		GetOrDefaultTemp(node, 24, "RetBad", 2, false)
		GetOrDefaultTemp(node, 25, "RetRechts", 2, false)
		GetOrDefaultTemp(node, 26, "RetLinks", 2, false)

	} else if node.NodeId == 6 {

		// GetOrDefaultProp(node, 52, "dimmer", 0, 60, 0, 0, true)
		// GetOrDefaultProp(node, 53, "defLevel", 0, 3600, 0, 0, true)
		// GetOrDefaultProp(node, 11, "vin", 1, 60, 0, 0, true)

	} else if nodeID == 7 {
		GetOrDefaultProp(node, 9, "IP", 0, 60, 0, 0, true)
		GetOrDefaultProp(node, 13, "button", 0, 60, 0, 0, true)
		GetOrDefaultProp(node, 20, "version", 0, 60, 0, 0, true)

		GetOrDefaultProp(node, 21, "vbLaag", 3, 60, 1, 0, true)
		GetOrDefaultProp(node, 51, "deltaLaag", 3, 60, 0, 0, true)
		GetOrDefaultProp(node, 22, "vbHoog", 3, 60, 1, 0, true)
		GetOrDefaultProp(node, 52, "deltaHoog", 3, 60, 0, 0, true)

		GetOrDefaultProp(node, 23, "retLaag", 3, 60, 0, 0, true)
		GetOrDefaultProp(node, 24, "retHoog", 3, 60, 0, 0, true)

		GetOrDefaultProp(node, 25, "vbGas", 3, 60, 1, 0, true)

		GetOrDefaultProp(node, 26, "tarief", 0, 60, 0, 0, true)

		GetOrDefaultProp(node, 29, "L1Pow", 3, 60, 0, 0, true)
		GetOrDefaultProp(node, 30, "L1PowNeg", 3, 60, 0, 0, true)
		GetOrDefaultProp(node, 31, "L1Curr", 0, 60, 0, 0, true)
		GetOrDefaultProp(node, 32, "L1Volt", 1, 60, 0, 0, true)

		GetOrDefaultProp(node, 33, "L2Pow", 3, 60, 0, 0, true)
		GetOrDefaultProp(node, 34, "L2PowNeg", 3, 60, 0, 0, true)
		GetOrDefaultProp(node, 35, "L2Curr", 0, 60, 0, 0, true)
		GetOrDefaultProp(node, 36, "L2Volt", 1, 60, 0, 0, true)

		GetOrDefaultProp(node, 37, "L3Pow", 3, 60, 0, 0, true)
		GetOrDefaultProp(node, 38, "L3PowNeg", 3, 60, 0, 0, true)
		GetOrDefaultProp(node, 39, "L3Curr", 0, 60, 0, 0, true)
		GetOrDefaultProp(node, 40, "L3Volt", 1, 60, 0, 0, true)

	} else if node.NodeId == 8 {
		GetOrDefaultProp(node, 55, "ssrPower", 0, 60, 0, 0, true)
		GetOrDefaultProp(node, 52, "maxTemp", 0, 3600, 0, 0, true)
		GetOrDefaultProp(node, 53, "maxSSRTemp", 0, 3600, 0, 0, true)

		GetOrDefaultTemp(node, 11, "temp", 2, true)
		GetOrDefaultTemp(node, 54, "ssrTemp", 2, true)

	} else if node.NodeId == 11 {
		GetOrDefaultTemp(node, 12, "kamer", 2, true)

	} else if node.NodeId == 14 {
		GetOrDefaultProp(node, 54, "total", 0, 60, 0, 0, true)
		GetOrDefaultProp(node, 55, "today", 2, 60, 0, 0, true)
		GetOrDefaultProp(node, 56, "temp", 1, 60, 0, 0, true)
		GetOrDefaultProp(node, 57, "power", 0, 60, 0, 0, true)

	} else if nodeID == 23 {
		GetOrDefaultTemp(node, 21, "buitenTemp", 2, true) // used by heating control!!
		GetOrDefaultTemp(node, 22, "douchTemp", 2, true)
		GetOrDefaultTemp(node, 23, "kraanTemp", 2, true)

		GetOrDefaultTemp(node, 24, "tempBinn", 2, true)          //"C,"
		GetOrDefaultProp(node, 25, "humBinn", 0, 60, 2, 0, true) //"%,"
		GetOrDefaultProp(node, 43, "absBinn", 0, 60, 2, 0, true) //"g/m3<br/>"

		GetOrDefaultTemp(node, 40, "tempBui", 2, true)          //"C,"
		GetOrDefaultProp(node, 41, "humBui", 2, 60, 2, 0, true) //"%,"
		GetOrDefaultProp(node, 42, "absBui", 2, 60, 2, 0, true) //"g/m3<br/>"

		GetOrDefaultProp(node, 44, "median", 2, 60, 1, 0, true) //"%"

		GetOrDefaultProp(node, 30, "manual", 0, 60, 0, 0, true)
		GetOrDefaultProp(node, 31, "fan", 0, 60, 1, 0, true)
		GetOrDefaultProp(node, 32, "maxHum", 0, 3600, 0, 0, true)
		GetOrDefaultProp(node, 33, "hotHum", 0, 3600, 0, 0, true)
		GetOrDefaultProp(node, 34, "coldHum", 0, 3600, 0, 0, true)
		GetOrDefaultProp(node, 35, "deltaHum", 0, 3600, 0, 0, true)

	}

	return node
}

func NodesFromDB() {

	var err error

	stmt, errr := DatabaseNew.Prepare(`Select nodeId, name, connId, nodeType from nodesDef where nodeId >= 0`)
	checkError(errr)

	defer stmt.Close()

	rows, errr := stmt.Query()
	if errr != nil {
		checkError(err)
	}

	defer rows.Close()

	var nodeID = 0
	var name string
	connID := 0
	nodeType := 0

	for rows.Next() {
		err := rows.Scan(&nodeID, &name, &connID, &nodeType)
		checkError(err)

		node := GetNode(nodeID)
		node.Name = name
		node.ConnId = connID
		node.IsDefSaved = true
		node.NodeType = nodeType
		node.IsInit = true
		// fmt.Printf("node:%#v", node)
	}

	if err = rows.Err(); err != nil {
		checkError(err)
	}

	err = rows.Close()
	checkError(err)

	if errr := rows.Err(); errr != nil {
		checkError(errr)
	}

	err = stmt.Close()
	checkError(err)
}

func NodeValuesFromDB(table string) {

	var err error

	stmt, errr := DatabaseNew.Prepare(fmt.Sprintf(`Select nodeid, boottime, bootcount, timestamp from %s where nodeId >= 0`, table))

	checkError(errr)

	defer stmt.Close()

	rows, errr := stmt.Query()
	if errr != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	nodeID := 0

	var boottime int64
	bootcount := 0
	var timestamp int64

	for rows.Next() {
		err := rows.Scan(&nodeID, &boottime, &bootcount, &timestamp)
		checkError(err)

		node := GetNode(nodeID)
		// if !node.IsInit {

		node.BootCount = bootcount
		node.Timestamp = timestamp
		node.IsDataSaved = true
		// node.IsInit = true
		// }
	}

	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	err = rows.Close()
	checkError(err)

	if errr := rows.Err(); errr != nil {
		checkError(errr)
	}

	err = stmt.Close()
	checkError(err)
}
