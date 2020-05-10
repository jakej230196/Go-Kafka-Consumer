package KafkaConsumer
import (
"fmt"
"github.com/confluentinc/confluent-kafka-go/kafka" 
"github.com/prometheus/client_golang/prometheus"
log "github.com/sirupsen/logrus"
"time"
"strings"
"path"
"runtime"
"os"
)
func GetLogFormatter() *log.JSONFormatter {
	Formatter := &log.JSONFormatter{
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			return f.Function, fmt.Sprintf("%s:%d", path.Base(f.File), f.Line)
		},
		FieldMap: log.FieldMap{
			log.FieldKeyFile:  "@file",
			log.FieldKeyTime:  "@timestamp",
			log.FieldKeyLevel: "@level",
			log.FieldKeyMsg:   "@message",
			log.FieldKeyFunc:  "@caller",
		},
	}
	Formatter.TimestampFormat = "2006-01-02T15:04:05.999999999Z"
	return Formatter
}

func LogLevel(lvl string) log.Level {
	level := strings.ToLower(lvl)
	switch level {
	case "debug":
		return log.DebugLevel
	case "info":
		return log.InfoLevel
	case "error":
		return log.ErrorLevel
	case "fatal":
		return log.FatalLevel
	default:
		panic(fmt.Sprintf("Log level (%s) is not supported", lvl))
	}
}
func init() {
    os.Setenv("LOG_LEVEL", "info")
	log.SetReportCaller(true)
	Formatter := GetLogFormatter()
	log.SetFormatter(Formatter)
	log.SetLevel(LogLevel(os.Getenv("LOG_LEVEL")))
}

func Start(KafkaServer string, KafkaTopic []string, ConsumerGroup string, InfoGauge *prometheus.GaugeVec, TotalErrorsReadGauge *prometheus.GaugeVec, ReadDurationHistogram *prometheus.HistogramVec) chan []byte {
	sigchan := make(chan os.Signal, 1)

	_Consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               KafkaServer,
		"group.id":                        ConsumerGroup,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"enable.partition.eof": true,
		"auto.offset.reset":    "earliest"})

	if err != nil {
		log.Fatal("Failed to create consumer: ", err)
	}
	fmt.Printf("Created Consumer %v\n", _Consumer)
	err = _Consumer.SubscribeTopics(KafkaTopic, nil)
	run := true
	OutputChannel := make(chan []byte, 1000)
    go func () {
		InfoGauge.With(prometheus.Labels{"queue": KafkaTopic[0], "worker": "consumer"}).Set(1)
        defer close(OutputChannel)
	    for run == true {
		    select {
		    case sig := <-sigchan:
			    log.Info("Caught signal", sig, ": terminating\n")
			    run = false
		    case ev := <-_Consumer.Events():
                ReadStartTime := time.Now()
			    switch e := ev.(type) {
			    case kafka.AssignedPartitions:
				    log.Info("%% ", e, "\n")
				    _Consumer.Assign(e.Partitions)
			    case kafka.RevokedPartitions:
				    log.Info("%% ", e, "\n")
				    _Consumer.Unassign()
				case *kafka.Message:
					OutputChannel <- e.Value
					ReadDurationHistogram.With(prometheus.Labels{"queue": KafkaTopic[0], "worker": "consumer"}).Observe(time.Since(ReadStartTime).Seconds())
		    	case kafka.PartitionEOF:
		    		log.Info("%% Reached ", e, "\n")
		    	case kafka.Error:
					TotalErrorsReadGauge.With(prometheus.Labels{"queue": KafkaTopic[0], "worker": "producer"}).Inc()
	    			log.Info("%% Error: ", e, "\n")
		    	}
	    	}
	    }
        close(OutputChannel)
    }()
    return OutputChannel 
}
