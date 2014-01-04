// AccuWeatherForecastsHourly project main.go
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"redis"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

//配置文件名称
const setting_file_name = "./settings.properties"

//go程数量
var complicate_count int = 0

//日志文件
var logger *log.Logger = nil
var savePath string = ""
var logFileName string = ""

//redis
var host string = ""
var port int = 6379
var password string = ""

//apikey
var apikey string = ""

//城市请求key值
var cityInfo string = ""

//管道
var end chan int
var city chan City //城市信息传送主管道
//任务记数
var taskCount int

//任务完成数据
var finishCount int = 0
var l sync.Mutex

//City类
type City struct {
	Id      string
	AccuKey string
	Path    string
	Count   int
}

/*************************/
//bool 代表 JSON booleans,
//float64 代表 JSON numbers,
//string 代表 JSON strings,
//nil 代表 JSON null.
/************************/
type Hour struct {
	DateTime            string
	WeatherIcon         float64
	IconPhrase          string
	Temperature         interface{}
	RealFeelTemperature interface{}
	RelativeHumidity    float64
	Unit                string
}
type Hourly struct {
	Hours []Hour
}

func main() {

	//读取配置文件
	settings, _ := readSetting(setting_file_name)
	savePath = settings["logPath"]
	complicate_count, _ = strconv.Atoi(settings["complicateNum"])
	apikey = settings["apiKey"]
	logFileName = settings["logFileName"]
	cityInfo = settings["cityInfo"]
	//redis配置
	host = settings["host"]
	port, _ = strconv.Atoi(settings["port"])
	password = settings["password"]

	//配置日志保存文件
	t := time.Now()
	logger, _ = setLoggerSaveFile(savePath, savePath+logFileName+"."+strconv.Itoa(t.Year())+"-"+strconv.Itoa(int(t.Month()))+"-"+strconv.Itoa(t.Day()))
	logger.Println("核心数：" + strconv.Itoa(runtime.NumCPU()) + "协程数：" + strconv.Itoa(complicate_count))
	//设置核心数
	runtime.GOMAXPROCS(runtime.NumCPU())
	cities, _ := readFileArray(cityInfo)
	taskCount = len(cities)
	city = make(chan City, complicate_count*2)
	defer close(city)
	end = make(chan int)
	defer close(end)
	go writeCitiesToChannel(city, cities)
	//开启Go程
	for i := 0; i < complicate_count; i++ {
		go startRequest(city)
	}
	go checkFinish()

	if <-end > 0 {
		logger.Println("任务执行完成一次")
	}
}
func checkFinish() {
	for {
		if finishCount == taskCount {
			end <- 1
			break
		}
		time.Sleep(time.Second * 10)
	}
}
func writeCitiesToChannel(city chan City, cities []City) {
	for i := 0; i < len(cities); i++ {
		city <- cities[i]
	}
	logger.Println("城市信息写入channel完成,启动结束计时...2分钟")
	//启动任务结束计时
	go func() {
		time.Sleep(time.Second * 60 * 2)
		end <- 1
	}()
}

//发送http请求
func startRequest(ch chan City) {
	client := &http.Client{}
	var redisclient redis.Client
	var rediserr error
	spec := redis.DefaultSpec().Db(0).Host(host).Port(port).Password(password)
	redisclient, rediserr = redis.NewSynchClientWithSpec(spec)
	if rediserr != nil {
		logger.Println("Redis连接错误！")
	}
	for {
		city := <-ch
		if len(city.Id) == 0 || len(city.AccuKey) == 0 {
			continue
		}
		resp, err := client.Get("http://apidev.accuweather.com/forecasts/v1/hourly/24hour/" + city.AccuKey + ".json?apiKey=" + apikey + "&language=en&details=true")
		if nil != err {
			logger.Println("城市：" + city.Id + "请求失败：" + city.AccuKey + "已返回请求队列")
			if city.Count <= 1 {
				ch <- city
				city.Count++
			}
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		if nil != err || len(body) == 0 {
			logger.Println("获取内容失败！")
			if city.Count <= 1 {
				ch <- city
				city.Count++
			}
			continue
		}
		resp.Body.Close()
		//save datas
		var hourly Hourly
		var data_24 string
		err = json.Unmarshal(body, &hourly.Hours)
		if err != nil {
			logger.Println("城市：" + city.Id + "解析响应失败，已返回队列！")
			if city.Count <= 2 {
				city.Count++
				ch <- city
			}
			continue
		}
		//保存数据
		for _, v := range hourly.Hours {
			data_Tmperature, _ := v.Temperature.(map[string]interface{})
			data_RealFeelTemperature, _ := v.RealFeelTemperature.(map[string]interface{})
			var temp string
			temp = v.DateTime + "," + strconv.FormatFloat(v.WeatherIcon, 'f', 1, 64) + "," + v.IconPhrase + "," + strconv.FormatFloat(v.RelativeHumidity, 'f', 1, 64) + "," + strconv.FormatFloat(data_Tmperature["Value"].(float64), 'f', 1, 64) + "," + strconv.FormatFloat(data_RealFeelTemperature["Value"].(float64), 'f', 1, 64) + "," + data_Tmperature["Unit"].(string)
			hour := v.DateTime[11:13]
			err := redisclient.Set("forecasts:hourly:"+city.Id+":"+hour, []byte(temp))
			if err != nil {
				fmt.Println(err)
			}
			data_24 += "#" + temp
		}
		err = redisclient.Set("forecasts:hourly:"+city.Id+":24", []byte(data_24[1:]))
		if err != nil {
			fmt.Println(err)
		}
		l.Lock()
		finishCount++
		l.Unlock()
	}
}

//设置日志保存路径和文件文件名
func setLoggerSaveFile(filePath string, fileName string) (loger *log.Logger, err error) {
	dirErr := os.MkdirAll(filePath, 0660)
	if dirErr != nil {
		fmt.Println("日志文件目录创建失败！")
		return nil, dirErr
	} else {
		logfile, fileErr := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660)
		if fileErr != nil {
			fmt.Println("打开日志保存文件失败！")
			return nil, fileErr
		}
		var logger *log.Logger
		logger = log.New(logfile, "", log.Ldate|log.Ltime)
		return logger, nil
	}
}

//读取配置文件方法
func readSetting(fileName string) (setting map[string]string, err error) {
	//#开头的正则表达式
	reg := regexp.MustCompile(`^#.*`)
	settings := make(map[string]string)
	settingFile, err := os.OpenFile(fileName, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	settingReader := bufio.NewReader(settingFile)
	for {
		str, _, err := settingReader.ReadLine()
		if err != nil {
			if io.EOF == err {
				break
			} else {
				fmt.Println("读取配置文件错误")
				break
			}
		}
		content := string(str[:])
		if 0 == len(content) || "\r\n" == content || reg.FindAllString(content, -1) != nil {
			continue
		}
		items := strings.Split(strings.TrimSpace(content), "=")
		settings[items[0]] = items[1]
	}
	return settings, nil
}

//读入城市请求key值
func readFileArray(fileName string) (result []City, err error) {
	var cities = make([]City, 0)
	srcFile, err := os.OpenFile(fileName, os.O_RDONLY, 0440)
	if nil != err {
		logger.Println("打开城市信息文件失败")
		return nil, err
	}
	defer srcFile.Close()
	srcReader := bufio.NewReader(srcFile)
	for {
		str, _, err := srcReader.ReadLine()
		if nil != err {
			if io.EOF == err {
				break
			} else {
				logger.Println("读取城市信息文件发生错误")
			}
		}
		content := string(str[:])
		if 0 == len(content) || "\r\n" == content {
			continue
		}
		var city City
		items := strings.Split(strings.TrimSpace(content), ",")
		if len(items) == 2 {
			city.Id = items[0]
			city.AccuKey = items[1]
			bucket, _ := strconv.Atoi(items[0])
			city.Path = "/" + strconv.Itoa(bucket%100) + "/"
			city.Count = 0
			cities = append(cities, city)
		}
	}
	return cities, nil
}
