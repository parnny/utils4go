package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/cihub/seelog"
)

const (
	defaultFilePermissions      = 0666
	defaultDirectoryPermissions = 0767
)

const (
	LogCloseReasonTimeout = 1
	LogCloseReasonSizeout = 2
	LogCloseReasonExit = 3
)

const (
	TimestepSecond = 1
	TimestepMinute = 60 * TimestepSecond
	TimestepHour = 60 * TimestepMinute
	TimestepDay	= 24 * TimestepHour
)

type FlashlogInfo struct {
	Timeblock		int
	Timezone		[2]int64
	Filepath 		string
	Temppath  		string
	Maxsize   		int64
	FileValid 		[2]int64
}

func GenFlashlogInfo(timestamp int64, timestep int, filesize int64, prefix string, suffix string) *FlashlogInfo {
	tm_msg := time.Unix(timestamp,0)
	tm_now := time.Now()

	flsinfo := new(FlashlogInfo)
	flsinfo.Timeblock = 0
	flsinfo.Maxsize = filesize

	flsinfo.Filepath = fmt.Sprintf( "%s/%d/%d/%d/",
		prefix,
		tm_msg.Year(),
		tm_msg.Month(),
		tm_msg.Day())

	if timestep < TimestepMinute {
		step_by_sec := timestep
		step_length := int64(step_by_sec)

		flsinfo.Filepath += fmt.Sprintf("%d/%d/%d/", tm_msg.Hour(), tm_msg.Minute(), tm_msg.Second())
		flsinfo.Timeblock = tm_msg.Second() / step_by_sec

		tm_vaild := time.Date(
			tm_now.Year(),tm_now.Month(),tm_now.Day(),
			tm_now.Hour(), tm_now.Minute(), flsinfo.Timeblock*step_by_sec,
			0,time.Local)
		flsinfo.FileValid[0] = tm_vaild.Unix()
		flsinfo.FileValid[1] = tm_vaild.Unix() + step_length

		tm_zone := time.Date(
			tm_msg.Year(),tm_msg.Month(),tm_msg.Day(),
			tm_msg.Hour(), tm_msg.Minute(), flsinfo.Timeblock*step_by_sec,
			0,time.Local)
		flsinfo.Timezone[0] = tm_zone.Unix()
		flsinfo.Timezone[1] = tm_zone.Unix() + step_length

	} else if timestep < TimestepHour {
		step_by_min := int(timestep/TimestepMinute)
		step_length := int64(step_by_min * TimestepMinute)

		flsinfo.Filepath += fmt.Sprintf("%d/%d/", tm_msg.Hour(), tm_msg.Minute())
		flsinfo.Timeblock = tm_msg.Minute() / step_by_min

		tm_vaild := time.Date(
			tm_now.Year(),tm_now.Month(),tm_now.Day(),
			tm_now.Hour(), flsinfo.Timeblock*step_by_min, 0,
			0,time.Local)

		tm_zone := time.Date(
			tm_msg.Year(),tm_msg.Month(),tm_msg.Day(),
			tm_msg.Hour(), flsinfo.Timeblock*step_by_min, 0,
			0,time.Local)

		flsinfo.FileValid[0] = tm_vaild.Unix()
		flsinfo.FileValid[1] = tm_vaild.Unix() + step_length
		flsinfo.Timezone[0] = tm_zone.Unix()
		flsinfo.Timezone[1] = tm_zone.Unix() + step_length

	} else if timestep < TimestepDay {
		step_by_hour := int(timestep/TimestepHour)
		step_length := int64(step_by_hour * TimestepHour)

		flsinfo.Filepath += fmt.Sprintf("%d/", tm_msg.Hour())
		flsinfo.Timeblock = tm_msg.Hour() / step_by_hour

		tm_vaild := time.Date(
			tm_now.Year(),tm_now.Month(),tm_now.Day(),
			flsinfo.Timeblock*step_by_hour, 0, 0,
			0,time.Local)

		tm_zone := time.Date(
			tm_msg.Year(),tm_msg.Month(),tm_msg.Day(),
			flsinfo.Timeblock*step_by_hour, 0, 0,
			0,time.Local)

		flsinfo.FileValid[0] = tm_vaild.Unix()
		flsinfo.FileValid[1] = tm_vaild.Unix() + int64(step_by_hour * TimestepHour)
		flsinfo.Timezone[0] = tm_zone.Unix()
		flsinfo.Timezone[1] = tm_zone.Unix() + step_length

	} else {
		flsinfo.Timeblock = 0
		flsinfo.FileValid[0] = tm_now.Unix()
		flsinfo.FileValid[1] = tm_now.Unix() + TimestepHour
		flsinfo.Timezone[0] = tm_now.Unix()
		flsinfo.Timezone[1] = tm_now.Unix() + TimestepHour
	}

	if len(suffix) > 0 {
		flsinfo.Filepath += suffix
		if strings.Index(suffix,"/") != len(suffix) {
			flsinfo.Filepath += "/"
		}
	}
	flsinfo.Temppath = flsinfo.Filepath + "temp/"
	return flsinfo
}


type FlashlogObj struct {
	Fd 				*os.File		// 文件描述符
	RollLock        sync.Mutex
	Filesize		int64			// 文件大小
	Info			*FlashlogInfo
	Filename		string
}

func (obj *FlashlogObj)Init(flsinfo *FlashlogInfo) bool {
	obj.Info = flsinfo
	if len(obj.Info.Filepath) == 0 {
		obj.Info.Filepath = "."
	}
	obj.Filename = ""
	return true
}

func (obj *FlashlogObj) NeedToRoll() bool {
	return obj.Filesize >= obj.Info.Maxsize
}

func (obj *FlashlogObj)Close(code int) error  {
	return obj.Roll(code)
}

func (obj *FlashlogObj)Roll(code int) error {
	obj.Printf("[Flashlog] File (%s) rolling by (%d) \n",obj.Filename,code)
	if code == LogCloseReasonTimeout {
		obj.Printf("\t[LogCloseReasonTimeout] Now:%d, [%d,%d] \n",time.Now().Unix(),obj.Info.FileValid[0],obj.Info.FileValid[1])
	}

	err := obj.Fd.Close()
	if err != nil {
		return err
	}
	obj.Fd = nil

	// rename
	err = os.Rename(filepath.Join(obj.Info.Temppath, obj.Filename + ".tmp"), filepath.Join(obj.Info.Filepath, obj.Filename + ".log"))
	if err != nil {
		return err
	}

	return nil
}

func (obj *FlashlogObj) Write(bytes []byte) (n int, err error) {
	obj.RollLock.Lock()
	defer obj.RollLock.Unlock()

	if obj.NeedToRoll(){
		if err := obj.Roll(LogCloseReasonSizeout); err != nil{
			return 0, err
		}
	}

	if obj.Fd == nil {
		err := obj.CreateFile()
		if err != nil {
			log.Error(err)
			return 0, nil
		}
	}

	n, err = obj.Fd.Write(bytes)
	obj.Filesize += int64(n)

	return n, err
}


func (obj *FlashlogObj) CreateFile() error {
	var err error

	if len(obj.Info.Temppath) != 0 {
		err = os.MkdirAll(obj.Info.Temppath, defaultDirectoryPermissions)
		if err != nil {
			return err
		}
	}

	obj.Filename = fmt.Sprintf("%s", time.Now().Format("20060102150405"))
	fullpath := filepath.Join(obj.Info.Temppath, obj.Filename + ".tmp")

	obj.Fd, err = os.OpenFile(fullpath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, defaultFilePermissions)
	if err != nil {
		return err
	}

	stat, err := obj.Fd.Stat()
	if err != nil {
		obj.Fd.Close()
		obj.Fd = nil
		return err
	}

	obj.Filesize = stat.Size()

	return nil
}

func (obj *FlashlogObj) IsTimeout() bool {
	return obj.Info.FileValid[1] < (time.Now().Unix() - 5)
}

func (obj *FlashlogObj)Printf(format string, a ...interface{}) {
	fmt.Printf(format,a...)
}