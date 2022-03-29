package persistence

import (
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

// 对于单机多实例部署、aof持久化和redis自身造成io竞争
// 可以通过监控info persistence来从外部控制进行aof控制

// loading:0 服务器是否正在载入持久化文件

// rdb_changes_since_last_save:0 离最近一次生成rdb文件，写入命令的个数，即有多少个命令没有初始化
// rdb_bgsave_in_progress:0 服务器是否正在创建rdb文件
// rdb_last_save_time:1648548607 最近一次创建rdb文件的时间戳
// rdb_last_bgsave_status:ok 最近一次rdb持久化是否成功
// rdb_last_bgsave_time_sec:0 最近一次生成rdb文件使用的时间（单位为s）
// rdb_current_bgsave_time_sec:-1 如果服务器正在创建rdb文件，那么值记录的就是当前的创建操作已经耗费的时间（单位s）

// aof_enabled:1  是否开启了aof，默认为0（没开启）
// aof_rewrite_in_progress:0  标识aof的rewrite操作是否在进行中
// aof_rewrite_scheduled:0  rewrite任务计划，当客户端发送bgrewriteaof指令，如果当前rewrite子进程正在执行，那么将客户端请求的bgrewriteaof变为计划任务，待aof子进程结束后执行rewrite
// aof_last_rewrite_time_sec:0 最近一次aof rewrite耗费的时间（单位s）
// aof_current_rewrite_time_sec:-1 如果rewrite操作正在进行，则记录所使用的时间
// aof_last_bgrewrite_status:ok 上次bgrewriteaof操作的状态
// aof_last_write_status:ok 上次aof写入状态
// aof_current_size:56 aof当前大小，以字节（byte）为单位
// aof_base_size:56  aof上次启动或rewrite的大小
// aof_pending_rewrite:0  同上面的aof_rewrite_scheduled
// aof_buffer_length:0 aof buffer的大小
// aof_rewrite_buffer_length:0 aof rewrite buffer的大小
// aof_pending_bio_fsync:0 后台IO队列中等待fsync任务的个数
// aof_delayed_fsync:0 延迟的fsync计数器

const (
	growRate = 75 // aof的增长率
)

type PersistenceInfo struct {
	RdbBgsaveInProgress      bool
	RdbCurrentBgsaveTimeSec  int64
	AofEnabled               bool
	AofRewriteInProgress     bool
	AofRewriteScheduled      bool
	AofCurrentRewriteTimeSec int64
	AofCurrentSize           int64
	AofBaseSize              int64
}

type AOF struct {
	Clients []redis.Cmdable
	IsNext  chan struct{}
	Finish  chan struct{}
}

func (aof *AOF) CheckCanWriteAof() error {
	for {
		select {
		case _, ok := <-aof.Finish:
			if !ok {
				// 回收资源停止循环
				return nil
			}
		default:
			aof.canWriteAof()
			// 休眠10ms
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (aof *AOF) canWriteAof() error {
	for _, client := range aof.Clients {
		info, err := client.Info("persistence").Result()
		if err != nil {
			log.Printf("check client %d write aof err:%s", client.ClientID().Val(), err.Error())
			return err
		}
		log.Printf("client check persistence info %s", info)
		infos := strings.Split(strings.TrimSpace(info), "\n")
		statusInfo := &PersistenceInfo{}
		for _, persistenceInfo := range infos {
			err := aof.getPersistenceInfo(statusInfo, client, persistenceInfo)
			if err != nil {
				return err
			}
		}
		if !statusInfo.AofEnabled {
			log.Printf("client %d not aof", client.ClientID().Val())
			continue
		}
		go aof.canNext(client, aof.IsNext)
		<-aof.IsNext
	}
	return nil
}

func (aof *AOF) canNext(client redis.Cmdable, signal chan<- struct{}) {
	for {
		select {
		case _, ok := <-aof.Finish:
			if !ok {
				// 停止循环
				return
			}
		default:
			info, err := client.Info("persistence").Result()
			if err != nil {
				log.Printf("can next client %d write aof err:%s", client.ClientID().Val(), err.Error())
				aof.IsNext <- struct{}{}
				return
			}
			log.Printf("can next persistence info %s", info)
			infos := strings.Split(strings.TrimSpace(info), "\n")
			statusInfo := &PersistenceInfo{}
			for _, persistenceInfo := range infos {
				err := aof.getPersistenceInfo(statusInfo, client, persistenceInfo)
				if err != nil {
					log.Printf("can next client %d  para persistence info err:%s", client.ClientID().Val(), err.Error())
					aof.IsNext <- struct{}{}
					return
				}
			}

			inprocess := (!statusInfo.RdbBgsaveInProgress) && (!statusInfo.AofRewriteInProgress)
			if inprocess && (!statusInfo.AofRewriteScheduled) {
				// 无bgsave 运行 无aof运行 bg结束后不运行 aof
				aof.IsNext <- struct{}{}
				return
			}

			// 无bgsave，无aof，并且bg结束后运行aof
			if inprocess && statusInfo.AofRewriteScheduled {
				// bgsave 运行完毕 是否运行 aof
				isBgSave := statusInfo.AofBaseSize == 0
				if !isBgSave {
					// 当base > 0 检测增长率是否满足触发条件
					rate := int((statusInfo.AofCurrentSize-statusInfo.AofBaseSize)/statusInfo.AofBaseSize) * 100
					isBgSave = rate > growRate
				}
				if isBgSave {
					// 手动触发bgaof
					client.BgRewriteAOF()
				}
			}
			// 休眠10ms
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (aof *AOF) getPersistenceInfo(statusInfo *PersistenceInfo, client redis.Cmdable, info string) error {
	persistences := strings.Split(strings.TrimSpace(info), ":")
	if len(persistences) < 2 {
		return nil
	}
	statusKey := persistences[0]
	statusValue := persistences[1]
	switch statusKey {
	case "rdb_bgsave_in_progress":
		val, err := strconv.ParseBool(statusValue)
		if err != nil {
			log.Printf("client %d get rdb_bgsave_in_progress err:%s", client.ClientID().Val(), err.Error())
			return err
		}
		statusInfo.RdbBgsaveInProgress = val
	case "rdb_current_bgsave_time_sec":
		val, err := strconv.ParseInt(statusValue, 10, 64)
		if err != nil {
			log.Printf("client %d get rdb_current_bgsave_time_sec err:%s", client.ClientID().Val(), err.Error())
			return err
		}
		statusInfo.RdbCurrentBgsaveTimeSec = val
	case "aof_enabled":
		val, err := strconv.ParseBool(statusValue)
		if err != nil {
			log.Printf("client %d get aof_enabled err:%s", client.ClientID().Val(), err.Error())
			return err
		}
		statusInfo.AofEnabled = val
	case "aof_rewrite_in_progress":
		val, err := strconv.ParseBool(statusValue)
		if err != nil {
			log.Printf("client %d get aof_rewrite_in_progress err:%s", client.ClientID().Val(), err.Error())
			return err
		}
		statusInfo.AofRewriteInProgress = val
	case "aof_rewrite_scheduled":
		val, err := strconv.ParseBool(statusValue)
		if err != nil {
			log.Printf("client %d get aof_rewrite_scheduled err:%s", client.ClientID().Val(), err.Error())
			return err
		}
		statusInfo.AofRewriteScheduled = val
	case "aof_current_rewrite_time_sec":
		val, err := strconv.ParseInt(statusValue, 10, 64)
		if err != nil {
			log.Printf("client %d get aof_current_rewrite_time_sec err:%s", client.ClientID().Val(), err.Error())
			return err
		}
		statusInfo.AofCurrentRewriteTimeSec = val
	case "aof_current_size":
		val, err := strconv.ParseInt(statusValue, 10, 64)
		if err != nil {
			log.Printf("client %d get aof_current_size err:%s", client.ClientID().Val(), err.Error())
			return err
		}
		statusInfo.AofCurrentSize = val
	case "aof_base_size":
		val, err := strconv.ParseInt(statusValue, 10, 64)
		if err != nil {
			log.Printf("client %d get aof_base_size err:%s", client.ClientID().Val(), err.Error())
			return err
		}
		statusInfo.AofBaseSize = val
	}
	return nil
}
