package iot

import (
	"bytes"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"time"
)

// SetStrProp ...
func SetStrProp(prop *IotProp, newVal string, timestamp int64) string {

	if LogProp(prop) {
		Info.Printf("SetStrProp isStr=%t n%dp%d:%d > %s ts:%d \n", prop.IsStr, prop.Node.NodeId, prop.PropId, prop.Val, newVal, timestamp)
	}

	if prop.IsStr {
		return SaveStrProp(prop, newVal, timestamp)
	}

	i64, err := strconv.ParseInt(newVal, 10, 32)
	if err != nil {
		Err.Printf("SetStrProp err parsing newVal to Int for prop:%v", prop)
		return fmt.Sprintf("SetStrProp err parsing newVal to Int for prop:%v", prop)
	}

	return SetProp(prop, i64, timestamp)

}

// SetProp ...
func SetProp(prop *IotProp, newVal int64, timestamp int64) string {

	//Info.Printf("SetProp %v newVal:%d\n", prop, newVal)
	if LogProp(prop) {
		Info.Printf("SetProp n%dp%d:%d > %d ts:%d \n", prop.Node.NodeId, prop.PropId, prop.Val, newVal, timestamp)
	}

	if prop.ValStamp > timestamp &&
		timestamp > 0 {
		errTxt := fmt.Sprintf("SaveProp:%v more recent then update:%d\n", prop, timestamp)
		Err.Printf(errTxt)
		return errTxt
	}

	if prop.Val == newVal && prop.ValStamp == timestamp {

		if LogProp(prop) {
			Err.Printf("SetProp skip duplicate n%dp%d:%d > %d ts:%d \n", prop.Node.NodeId, prop.PropId, prop.Val, newVal, timestamp)
		}

		return ""
	}
	if prop.Node.NodeId < 3 ||
		prop.Node.ConnId == 2 {

		return SaveProp(prop, newVal, timestamp)

	} else if prop.Node.ConnId > 2 {
		return SendProp(prop, "S", strconv.FormatInt(newVal, 10))
		// return iot.SendCmd(iotPayload)

	} else {
		err := fmt.Sprintf("SetProp connId missing prop:%v \n", prop)
		Err.Println(err)
		return err

	}
}

// SaveStrProp ...
func SaveStrProp(prop *IotProp, newVal string, timestamp int64) string {

	if prop.IsStr {

		if LogProp(prop) {
			Info.Printf("SaveStrProp n%dp%d:%s > %s ts:%d \n", prop.Node.NodeId, prop.PropId, prop.ValString, newVal, timestamp)
		}

		if timestamp == 0 {
			timestamp = time.Now().Unix()
		} else if timestamp > 3000000000 {
			timestamp = timestamp / 1000
		}

		if prop.ValStamp > timestamp &&
			timestamp > 0 {
			errTxt := fmt.Sprintf("SaveProp:%v more recent then update:%d\n", prop, timestamp)
			Err.Printf(errTxt)
			return errTxt
		}

		if prop.ValString == newVal && prop.ValStamp == timestamp {

			if LogProp(prop) {
				Info.Printf("SaveStrProp.SkipDuplicate n%dp%d:%s > %s ts:%d \n", prop.Node.NodeId, prop.PropId, prop.ValString, newVal, timestamp)
			}
			return ""
		}

		prop.ValString = newVal
		return SavePropAfter(prop, timestamp)

	} else if len(newVal) < 1 {

		return SaveProp(prop, 0, time.Now().Unix())

	} else {

		i64, err := strconv.ParseInt(newVal, 10, 32)
		if err != nil {
			return fmt.Sprintf("Set newVal err parsing to Int for prop:%v", prop)
		}
		return SaveProp(prop, i64, timestamp)
	}
}

// SaveProp ...
func SaveProp(prop *IotProp, newVal int64, timestamp int64) string {

	// TODO additional check like min and max val or max delta

	if LogProp(prop) {
		Info.Printf("SaveProp n%dp%d:%d > %d ts:%d \n", prop.Node.NodeId, prop.PropId, prop.Val, newVal, timestamp)
	}

	if timestamp == 0 {
		timestamp = time.Now().Unix()
	} else if timestamp > 3000000000 {
		timestamp = timestamp / 1000
	}

	if prop.PropId == 0 || prop.PropId == 1 {
		prop.Node.BootStamp = timestamp
	}

	updateLogItemSucces := false

	if prop.LogOption > 0 {
		insertLogItem := false

		if prop.PrevStamp > 0 {

			if prop.Val == newVal &&
				prop.Val == prop.Prev {

				updateLogItemSucces = updateLogMem(prop, newVal, timestamp)
				if LogProp(prop) {
					Info.Printf("SaveProp.Log updateTimestamp n%dp%d:%d > %d val:%d \n", prop.Node.NodeId, prop.PropId, prop.ValStamp, timestamp, prop.Val)
				}

			} else if prop.LogOption > 1 {
				// if extrapolate 1 and 2 to 3. When 3 comes close then skip 2=the middle one
				partialTimeSpan := float64(prop.ValStamp - prop.PrevStamp)
				totalTimeSpan := float64(timestamp - prop.PrevStamp)
				partialDelta := float64(prop.Val - prop.Prev)
				realDelta := float64(newVal - prop.Prev)
				calculatedDelta := partialDelta * totalTimeSpan / partialTimeSpan
				if math.Abs(realDelta-calculatedDelta) < 0.6 {

					updateLogItemSucces = updateLogMem(prop, newVal, timestamp)
					if LogProp(prop) {
						Info.Printf("SaveProp.Log updateTimestamp(extrapolation) n%dp%d:%d > %d val:%d \n", prop.Node.NodeId, prop.PropId, prop.ValStamp, timestamp, prop.Val)
					}

				} else {
					insertLogItem = true
				}

			} else {
				insertLogItem = true
			}
		}

		if insertLogItem || !updateLogItemSucces {
			if LogProp(prop) {
				Info.Printf("SaveProp.Log add n%dp%d:%d > %d ts:%d \n", prop.Node.NodeId, prop.PropId, prop.Val, newVal, timestamp)
			}
			addLogMem(prop, newVal, timestamp)
		}
	}

	if !updateLogItemSucces {
		prop.Prev = prop.Val
		prop.PrevStamp = prop.ValStamp
	}

	prop.Val = newVal

	return SavePropAfter(prop, timestamp)
}

// SavePropAfter ...
func SavePropAfter(prop *IotProp, timestamp int64) string {

	if timestamp == 0 {
		timestamp = time.Now().Unix()
	} else if timestamp > 3000000000 {
		timestamp = timestamp / 1000
	}

	if timestamp > prop.Node.Timestamp {
		prop.Node.Timestamp = timestamp
		PersistNodeMem(prop.Node)
	}

	prop.ValStamp = timestamp
	prop.RetryCount = 0
	prop.InErr = false

	if prop.RefreshRate > 0 {
		prop.NextRefresh = prop.ValStamp + int64(prop.RefreshRate)

	} else {
		prop.NextRefresh = 0
	}

	// if(status < 0 && iotMsg.val == soll )
	// {
	// 	status = - status;
	// }

	if LogProp(prop) {

		if prop.IsStr {
			Info.Printf("SaveStrPropAfter n%dp%d:%s ts:%d next:%d\n", prop.Node.NodeId, prop.PropId, prop.ValString, timestamp, prop.NextRefresh)

		} else {
			Info.Printf("SavePropAfter n%dp%d:%d ts:%d next:%d\n", prop.Node.NodeId, prop.PropId, prop.Val, timestamp, prop.NextRefresh)
		}
	}

	OnNodePropSave(prop)

	MvcUp(prop)

	PersistPropToMem(prop)

	return ""
}

// SendCmd ...
func SendCmd(iotPayload *IotPayload) string {

	prop := GetProp(iotPayload.NodeId, iotPayload.PropId)

	return SendProp(prop, iotPayload.Cmd, iotPayload.Val)
}

// SendProp ...
func SendProp(prop *IotProp, cmd string, newVal string) string {

	if prop == nil {
		Err.Printf("SendProp prop==nil")
		return "SendProp prop==nil"
	}

	if LogProp(prop) {
		Info.Printf("SendProp cmd:%s n%dp%d:%d > %s \n", cmd, prop.Node.NodeId, prop.PropId, prop.Val, newVal)
	}

	if prop.Node.ConnId < 1 {
		err := fmt.Sprintf("sendProp.err: connId < 1 for %d-%d\n", prop.Node.NodeId, prop.PropId)
		Err.Printf(err)
		return err

	} else {
		if cmd == "set" {
			cmd = "S"
		}
		if newVal == "" {
			newVal = "0"
		}

		topic := MqttDown + "/" + strconv.Itoa(prop.Node.ConnId)
		payload := fmt.Sprintf("{%s,%d,%d,1,%s}", cmd, prop.Node.NodeId, prop.PropId, newVal)

		if LogProp(prop) {
			Info.Printf("SendProp %s [%s]\n", payload, topic) // {S,4,52,1,55} [iotOut/11]
		}

		token := MqttClient3b.Publish(topic, byte(0), false, payload)
		if token.Wait() && token.Error() != nil {
			Err.Printf("mq.3b error %s [%s]\n", payload, MqttDown)
			Err.Printf("Fail to publish, %v", token.Error())
		}

		token = MqttClient3.Publish(topic, byte(0), false, payload)
		if token.Wait() && token.Error() != nil {
			Err.Printf("mq.3error %s [%s]\n", payload, MqttDown)
			Err.Printf("Fail to publish, %v", token.Error())
		}
	}

	return ""
}

// GetProp ...
func GetProp(nodeID int, propID int) *IotProp {

	node := GetNode(nodeID)

	return GetNodeProp(node, propID)
}

// GetNodeProp ...
func GetNodeProp(node *IotNode, propID int) *IotProp {

	if prop, found := node.Props[propID]; found {
		// if prop.OnRead {
		// 	OnNodePropRead(prop)
		// }
		return prop

	} else {
		// varxID := node.NodeId*IDFactor + propID
		// VarxId:     varIxD,

		node.Props[propID] = &IotProp{
			NodeId:    node.NodeId,
			PropId:    propID,
			Node:      node,
			Decimals:  0,
			Name:      fmt.Sprintf("n%dp%d", node.NodeId, propID),
			LocalMvc:  false,
			GlobalMvc: false}

		return node.Props[propID]
	}
}

// GetOrDefaultTemp ...
func GetOrDefaultTemp(node *IotNode, propID int, name string, logOpt int, globalMvc bool) *IotProp {

	prop := GetNodeProp(node, propID)
	if !prop.IsInit {
		prop = GetOrDefaultProp(node, propID, name, 2, 60, logOpt, 0, globalMvc)
		prop.LogOption = 2
	}

	return prop
}

// GetOrDefaultProp ...
func GetOrDefaultProp(node *IotNode, propID int, name string, decimals int, refreshRate int, logOpt int, traceOpt int, globalMvc bool) *IotProp {

	// Info.Printf("GetOrDefaultProp n%dp%d %s", node.NodeId, propID, name)

	prop := GetProp(node.NodeId, propID)

	if !prop.IsInit {
		Info.Printf("GetOrDefaultProp.Init: n%dp%d", node.NodeId, propID)
		prop.Node = node
		prop.Decimals = decimals
		prop.RefreshRate = refreshRate
		prop.LogOption = logOpt
		prop.TraceOption = traceOpt
		prop.LocalMvc = true
		prop.GlobalMvc = globalMvc
		prop.Name = name
		prop.IsInit = true

		PersistPropDef(prop)
	}

	setNextRefresh(prop)

	if !prop.IsDataSaved {
		PersistPropToMem(prop)
	}

	return prop
}

func setNextRefresh(prop *IotProp) {

	if prop.RefreshRate > 0 {

		// repair timestamps in future
		if prop.ValStamp > time.Now().Unix() {
			prop.ValStamp = time.Now().Unix()
			prop.IsDataSaved = false
		}

		nextRefresh := prop.ValStamp + int64(prop.RefreshRate)
		if prop.RetryCount > 7 {
			nextRefresh = time.Now().Unix() + int64(prop.RefreshRate)*7

		} else if prop.RetryCount > 3 {
			nextRefresh = time.Now().Unix() + int64(prop.RefreshRate)*int64(prop.RetryCount)

		}

		if prop.NextRefresh == 0 ||
			nextRefresh < prop.NextRefresh {

			prop.NextRefresh = nextRefresh
			prop.IsDataSaved = false
			if LogProp(prop) || LogCode("R") {
				Trace.Printf("setNextRefresh n%dp%d set nextRefresh %d\n", prop.Node.NodeId, prop.PropId, nextRefresh)
			}
		} else {
			if LogProp(prop) || LogCode("R") {
				Trace.Printf("setNextRefresh n%dp%d keep nextRefresh %d\n", prop.Node.NodeId, prop.PropId, prop.NextRefresh)
			}
		}

	} else if prop.NextRefresh > 0 {

		prop.NextRefresh = 0
		prop.IsDataSaved = false
	}

}

func addLogMem(prop *IotProp, newVal int64, timeStamp int64) bool {

	append, err := DatabaseNew.Prepare(`
	INSERT INTO logMem( nodeid, propid, stamp, val, delta ) VALUES(?, ?, ?, ?, ?) 
	ON DUPLICATE KEY UPDATE val=?, delta=? `)

	defer append.Close()

	if err != nil {
		Err.Printf("addLogMem.err: %v\n", err.Error())
		return false
	}

	delta := newVal - prop.Val
	_, err = append.Exec(prop.NodeId, prop.PropId, timeStamp, newVal, delta, newVal, delta)

	if err != nil {
		Err.Printf("appendLogItem.err: %v\n", err.Error())
		return false
	}

	return true
}

func updateLogMem(prop *IotProp, newVal int64, newTimeStamp int64) bool {

	update, err := DatabaseNew.Prepare(`Update logMem set stamp = ?, val = ?, delta = ? where nodeid = ? and propid = ? and stamp = ?`)

	defer update.Close()

	if err != nil {
		Err.Printf("updateLogItem.err: %v\n", err.Error())
		return false
	}

	delta := newVal - prop.Val
	_, err = update.Exec(newTimeStamp, newVal, delta, prop.NodeId, prop.PropId, prop.ValStamp)

	if err != nil {
		Err.Printf("updateLogItem.err: %v\n", err.Error())
		return false
	}
	return true
}

func PersistPropDef(prop *IotProp) {
	//fmt.Printf("PersistPropDef n%dp%d   \n", prop.NodeId, prop.PropId)
	// TODO onRead, onSave, isStr

	persist, err := DatabaseNew.Prepare(`
	INSERT INTO propsDef( nodeId , 
		propId, 
		name, 
		decimals, 
		logOption, 
		traceOption, 
		localMvc,
		globalMvc,
		drawType,
		drawOffset,
		drawColor,
		drawFactor,
		refreshRate ) VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
	ON DUPLICATE KEY UPDATE  name=?, 
		decimals=?, 
		logOption=?, 
		traceOption=?, 
		localMvc=?,
		globalMvc=?,
		drawType=?,
		drawOffset=?,
		drawColor=?,
		drawFactor=?,
		refreshRate=? `)
	if err != nil {
		Err.Printf("persistPropDef: %v", err.Error())
	}

	defer persist.Close()

	_, err = persist.Exec(
		prop.NodeId,
		prop.PropId,
		prop.Name,
		prop.Decimals,
		prop.LogOption,
		prop.TraceOption,
		prop.LocalMvc,
		prop.GlobalMvc,
		prop.DrawType,
		prop.DrawOffset,
		prop.DrawColor,
		prop.DrawFactor,
		prop.RefreshRate,

		prop.Name,
		prop.Decimals,
		prop.LogOption,
		prop.TraceOption,
		prop.LocalMvc,
		prop.GlobalMvc,
		prop.DrawType,
		prop.DrawOffset,
		prop.DrawColor,
		prop.DrawFactor,
		prop.RefreshRate)

	if err != nil {
		Err.Printf("persistPropDef: %v", err.Error())
	}
	prop.IsDefSaved = true
}

func PersistPropToMem(prop *IotProp) {

	// if prop.Node.NodeId == 15 {
	// 	fmt.Printf("PersistPropToMem n%dp%d   \n", prop.NodeId, prop.PropId)
	// 	fmt.Printf("Prop %v\n", prop)

	// }

	/*
		err,
		err=?,

	*/

	persist, err := DatabaseNew.Prepare(`
	INSERT INTO propsMem( nodeId,
		propId,
		val, 
		valString, 
		valStamp, 
		inErr, 
		errStamp, 
		retryCnt, 
		nextRefresh) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?) 
	ON DUPLICATE KEY UPDATE val=?, 
		valString=?, 
		valStamp=?, 
		inErr=?, 

		errStamp=?, 
		retryCnt=?, 
		nextRefresh=? `)

	if err != nil {
		Err.Printf("persistPropMem prep: %v\n", err.Error())
	}

	defer persist.Close()

	// var nextRefresh int64
	// if prop.RefreshRate > 0 {
	// 	nextRefresh = time.Now().Unix() + int64(prop.RefreshRate)
	// }

	_, err = persist.Exec(
		prop.NodeId,
		prop.PropId,
		prop.Val,
		prop.ValString,
		prop.ValStamp,
		prop.InErr,

		prop.ErrStamp,
		prop.RetryCount,
		prop.NextRefresh,
		prop.Val,
		prop.ValString,
		prop.ValStamp,
		prop.InErr,

		prop.ErrStamp,
		prop.RetryCount,
		prop.NextRefresh)

	if err != nil {
		Err.Printf("persistPropMem exec: %v\n", err.Error())
	}

	prop.IsDataSaved = true
}

func PropRecent(prop *IotProp) int {

	recent := 01 // 2 oranje, 3 rood

	if prop.RefreshRate > 0 {
		recent = 3

		if prop.ValStamp >= time.Now().Unix()-int64(2*prop.RefreshRate) {
			recent = 1

		} else if prop.ValStamp >= time.Now().Unix()-int64(3*prop.RefreshRate) {
			recent = 2
		}
	}

	return recent
}

func PropMvc(respBuff *bytes.Buffer, prop *IotProp, global bool) {

	empty := respBuff.Len() < 1
	comma := ","
	if empty {
		comma = ""
	}

	if prop.IsInit {

		recent := PropRecent(prop)

		if prop.IsStr {
			if global {
				respBuff.Write([]byte(fmt.Sprintf(`%s"n%dp%d":["%s",%d]`, comma, prop.NodeId, prop.PropId, prop.ValString, recent)))
			} else {
				respBuff.Write([]byte(fmt.Sprintf(`%s"p%d":["%s",%d]`, comma, prop.PropId, prop.ValString, recent)))
			}

		} else if prop.Decimals == 0 {
			if global {
				respBuff.Write([]byte(fmt.Sprintf(`%s"n%dp%d":[%d,%d]`, comma, prop.NodeId, prop.PropId, prop.Val, recent)))
			} else {
				respBuff.Write([]byte(fmt.Sprintf(`%s"p%d":[%d,%d]`, comma, prop.PropId, prop.Val, recent)))
			}

		} else if prop.Decimals == 1 || prop.Decimals == 2 || prop.Decimals == 3 {

			factor := math.Pow10(prop.Decimals)

			if global {
				respBuff.Write([]byte(fmt.Sprintf(`%s"n%dp%d":[%.1f,%d]`, comma, prop.NodeId, prop.PropId, float64(prop.Val)/factor, recent)))
			} else {
				respBuff.Write([]byte(fmt.Sprintf(`%s"p%d":[%.1f,%d]`, comma, prop.PropId, float64(prop.Val)/factor, recent)))
			}

		} else {
			if global {
				respBuff.Write([]byte(fmt.Sprintf(`%s"n%dp%d":[%d,%d]`, comma, prop.NodeId, prop.PropId, prop.Val, recent)))
			} else {
				respBuff.Write([]byte(fmt.Sprintf(`%s"p%d":[%d,%d]`, comma, prop.PropId, prop.Val, recent)))
			}
		}
	}
}

func PropsFromDB() {

	var err error

	stmt, errr := DatabaseNew.Prepare(`Select nodeId, propId, name, refreshRate, decimals, drawType, drawColor, drawOffset, drawFactor, logOption, traceOption, globalMvc, localMvc, isStr, onRead, onSave from propsDef where nodeId >= 0`)
	checkError(errr)

	defer stmt.Close()

	rows, errr := stmt.Query()
	if errr != nil {
		checkError(err)
	}

	defer rows.Close()

	nodeID := 0
	propID := 0
	var name string

	var decimals sql.NullInt64
	var refreshRate sql.NullInt64

	var drawType sql.NullInt64
	var drawColor sql.NullString
	var drawOffset sql.NullInt64
	var drawFactor sql.NullFloat64

	var logOption sql.NullInt64
	var traceOption sql.NullInt64

	var globalMvc sql.NullInt64
	var localMvc sql.NullInt64
	var isStr sql.NullInt64
	var onRead sql.NullInt64
	var onSave sql.NullInt64

	for rows.Next() {
		err := rows.Scan(&nodeID, &propID, &name, &refreshRate, &decimals, &drawType, &drawColor, &drawOffset, &drawFactor, &logOption, &traceOption, &globalMvc, &localMvc, &isStr, &onRead, &onSave)
		checkError(err)

		prop := GetProp(nodeID, propID)

		prop.Name = name
		SqlInt(&prop.RefreshRate, refreshRate)

		SqlInt(&prop.DrawType, drawType)
		SqlString(&prop.DrawColor, drawColor)
		SqlInt(&prop.DrawOffset, drawOffset)
		SqlFloat32(&prop.DrawFactor, drawFactor)
		SqlInt(&prop.Decimals, decimals)

		SqlInt(&prop.LogOption, logOption)
		SqlInt(&prop.TraceOption, traceOption)
		SqlBoolInt(&prop.GlobalMvc, globalMvc)
		SqlBoolInt(&prop.LocalMvc, localMvc)
		SqlBoolInt(&prop.IsStr, isStr)
		SqlBoolInt(&prop.OnRead, onRead)
		SqlBoolInt(&prop.OnSave, onSave)

		// if prop.OnRead {
		// 	OnNodePropRead(prop)
		// }

		prop.IsInit = true
		// fmt.Printf("prop:%#v", prop)
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

func PropValuesFromDB(table string) {

	var err error

	stmt, errr := DatabaseNew.Prepare(fmt.Sprintf(`Select nodeId, propId, val, valString, valStamp, nextRefresh, retryCnt, err, errStamp  from %s`, table))
	checkError(errr)

	defer stmt.Close()

	rows, errr := stmt.Query()
	if errr != nil {
		checkError(err)
	}

	defer rows.Close()

	nodeID := 0
	propID := 0
	var val sql.NullInt64
	var valString sql.NullString
	var valStamp sql.NullInt64
	var nextRefresh sql.NullInt64
	var retryCnt sql.NullInt64
	var error sql.NullString // value or isError
	var errStamp sql.NullInt64

	for rows.Next() {
		err := rows.Scan(&nodeID, &propID, &val, &valString, &valStamp, &nextRefresh, &retryCnt, &error, &errStamp)
		checkError(err)

		prop := GetProp(nodeID, propID)

		if !prop.IsDataSaved {
			SqlInt64(&prop.Val, val)
			SqlInt64(&prop.ValStamp, valStamp)
			SqlString(&prop.ValString, valString)

			SqlInt64(&prop.NextRefresh, nextRefresh)
			SqlInt(&prop.RetryCount, retryCnt)

			SqlString(&prop.Err, error)

			prop.IsDataSaved = true

			OnNodePropRead(prop)

		}

		// SqlInt64(&prop.ErrorTimeStamp, errorTimer)
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
