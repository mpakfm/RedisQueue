<?php
/**
 * Created by PhpStorm.
 * User: woojean
 * Date: 2018/2/28
 * Time: 10:04
 */

namespace Mpakfm\RedisQueue;

/**
 * @property \Redis redis
 */
class RedisQueue
{
    const PREFIX                 = 'RQ';
    const ERROR_QUEUE_NAME_EMPTY = 'Queue name can not be empty!';
    const PROCESSING_INDEX       = 'PI';
    const DATA_KEY               = 'DK';

    public $queueName;
    public $redis;

    public $retryTimes = 3;
    public $waitTime   = 3;


    /**
     * RedisQueue constructor.
     * @param $queueName
     * @param $redisConfig
     * [
     *   'host' => '127.0.0.1',
     *   'port' => '6379',
     *   'index' => 0,
     * ]
     * @param $retryTimes
     * @param $waitTime
     * @throws RedisQueueException
     */

    public function __construct(string $queueName, array $redisConfig, int $retryTimes = 3, int $waitTime = 3)
    {
        if (empty($queueName)) {
            throw new RedisQueueException('getCurrentIndex:' . self::ERROR_QUEUE_NAME_EMPTY);
        }

        if (empty($redisConfig)) {
            throw new RedisQueueException('Redis config name can not be empty!');
        }

        $this->queueName = $queueName;
        $this->retryTimes = $retryTimes;
        $this->waitTime = $waitTime;

        $ret = $this->init($redisConfig);
        if (false === $ret) {
            throw new RedisQueueException('Queue init failed!');
        }
    }

    public function getIndexListName(): string
    {
        // index list
        return self::PREFIX . ':IL:' . $this->queueName;
    }

    public function getBlockedListName(): string
    {
        // blocked list
        return self::PREFIX . ':BL:' . $this->queueName;
    }

    public function getDataHashName(): string
    {
        // data hash
        return self::PREFIX . ':DH:' . $this->queueName;
    }

    public function getBlockedTimesHashName(): string
    {
        // blocked times hash
        return self::PREFIX . ':BTH' . $this->queueName;
    }

    public function getDataKey(array $data)
    {
        return $data[self::DATA_KEY];
    }

    public function setDataKey(array $data, string $index): array
    {
        $data[self::DATA_KEY] = $index;
        return $data;
    }

    public function getProcessingIndexName(array $data): string
    {
        $dataKey = $this->getDataKey($data);
        $keyName = self::PREFIX . ':' . self::PROCESSING_INDEX . ':' . $this->queueName . ':' . $dataKey;
        return $keyName;
    }

    /**
     * @return string|mixed|bool If key didn't exist, FALSE is returned.
     * Otherwise, the value related to this key is returned
     */
    public function getProcessingIndex(array $data)
    {
        $keyName = $this->getProcessingIndexName($data);
        return $this->redis->get($keyName);
    }

    public function setProcessingIndex(array $data): bool
    {
        $keyName = $this->getProcessingIndexName($data);
        $index = $this->getDataKey($data);
        return $this->redis->set($keyName, $index);
    }

    public function removeProcessingIndex(array $data): int
    {
        $keyName = $this->getProcessingIndexName($data);
        return $this->redis->del($keyName);
    }

    public function removeAllProcessingIndex(): void
    {
        $pattern = self::PREFIX . ':' . self::PROCESSING_INDEX . ':' . $this->queueName . ':*';
        $keys = $this->redis->keys($pattern);
        foreach ($keys as $key) {
            $this->redis->del($key);
        }
    }

    public function init(array $redisConfig): bool
    {
        $this->redis = new \Redis();
        $this->redis->connect($redisConfig['host'], $redisConfig['port']);

        if(!empty($redisConfig['auth'])){
            $this->redis->auth($redisConfig['auth']);
        }

        if(!empty($redisConfig['index'])){
            $this->redis->select($redisConfig['index']);
        }
        return true;
    }

    public function genGuid(): string
    {
        return uniqid($this->queueName . '_');
    }

    public function addIndex(string $index): int
    {
        $ret = $this->redis->lpush($this->getIndexListName(), $index);
        if ($ret === false) {
            throw new RedisQueueException('Can not add index');
        }
        return $ret;
    }

    public function transferToBlocked(string $index): int
    {
        $ret = $this->redis->lpush($this->getBlockedListName(), $index);
        if ($ret === false) {
            throw new RedisQueueException('Can not transfer index');
        }
        return $ret;
    }

    /**
     * Removes and returns the last element of the list stored at key.
     * FALSE in case of failure (empty list)
     * @return mixed|bool
     */
    public function getIndex()
    {
        $ret = $this->redis->rPop($this->getIndexListName());
        return $ret;
    }

    /**
     * @return mixed
     */
    public function getData(string $index)
    {
        $ret  = $this->redis->hGet($this->getDataHashName(), $index);
        $data = json_decode($ret, true);
        return $data;
    }

    /**
     * @return bool|int
     */
    public function addData(string $index, array $data)
    {
        $data = json_encode($data, JSON_UNESCAPED_UNICODE);
        $ret  = $this->redis->hSet($this->getDataHashName(), $index, $data);
        return $ret;
    }

    /**
     * @return bool|int
     */
    public function removeData(array $data)
    {
        $index = $this->getDataKey($data);
        $ret   = $this->redis->hDel($this->getDataHashName(), $index);
        return $ret;
    }

    public function getBlockedTimes(string $processingIndex): int
    {
        $ret = $this->redis->hGet($this->getBlockedTimesHashName(), $processingIndex);
        return intval($ret);
    }

    /**
     * @return int|bool
     * - 1 if value didn't exist and was added successfully,
     * - 0 if the value was already present and was replaced, FALSE if there was an error.
     */
    public function addBlocked(string $processingIndex)
    {
        $blockedTime = $this->redis->hGet($this->getBlockedTimesHashName(), $processingIndex);
        $blockedTime += 1;
        $ret = $this->redis->hSet($this->getBlockedTimesHashName(), $processingIndex, $blockedTime);
        return $ret;
    }

    /**
     * @return  mixed|bool if command executed successfully BOOL FALSE in case of failure (empty list)
     */
    public function getBlockedIndex()
    {
        $ret = $this->redis->rPop($this->getBlockedListName());
        return $ret;
    }

    /**
     * @return int|bool Number of deleted fields
     */
    public function removeBlockedTimes(string $index)
    {
        $ret = $this->redis->hDel($this->getBlockedTimesHashName(), $index);
        return $ret;
    }

    // ==== API ====================================================================
    // add new message
    public function add(array $data): string
    {
        // make index
        $index = $this->genGuid();

        // add index
        $ret = $this->addIndex($index);
        if (false == $ret) {
            throw new RedisQueueException('Add index failed!');
        }

        // add data
        $ret = $this->addData($index, $data);
        if (false == $ret) {

            throw new RedisQueueException('Add data failed!');
        }

        return $index;
    }

    // get message
    public function get(): array
    {
        // get index
        $index = $this->getIndex();
        if (empty($index)) {
            sleep($this->waitTime);
            return null;
        }

        // get data
        $data = $this->getData($index);
        if (empty($data)) { // invalid index
            $data = $this->setDataKey($data, $index);
            $this->remove($index);
        }

        // set current key
        $data = $this->setDataKey($data, $index);
        $this->setProcessingIndex($data);

        return $data;
    }

    /**
     * remove message
     * @return bool|int
     */
    public function remove(array $data)
    {
        // remove processing index
        $this->removeProcessingIndex($data);

        // remove data
        $ret = $this->removeData($data);
        return $ret;
    }

    // rollback message
    public function rollback(array $data): void
    {
        $processingIndex = $this->getProcessingIndex($data);
        if ($processingIndex) {
            // add blocked times
            $this->addBlocked($processingIndex);

            // check blocked times
            $blockTimes = $this->getBlockedTimes($processingIndex);
            if ($blockTimes >= $this->retryTimes) { // if retry times up to max, the index will be transfer to blocked list
                // transfer to blocked list
                $ret = $this->transferToBlocked($processingIndex);
            } else {
                // rollback index
                $ret = $this->addIndex($processingIndex);
            }
            if (!empty($ret)) {
                // clear processing index
                $this->removeProcessingIndex($data);
            }
        }
    }

    /**
     * processing
     * @return string|mixed|bool If key didn't exist, FALSE is returned.
     * Otherwise, the value related to this key is returned
     */
    public function getCurrentIndex(array $data)
    {
        $processingIndex = $this->getProcessingIndex($data);
        return $processingIndex;
    }

    /**
     * repair
     */
    public function repair(): int
    {
        $num = 0;
        // restore blocked
        $blockedIndex = $this->getBlockedIndex();
        while (!empty($blockedIndex)) {
            $this->addIndex($blockedIndex);

            // clear blocked times
            $this->removeBlockedTimes($blockedIndex);

            $blockedIndex = $this->getBlockedIndex();
            $num += 1;
        }

        // clear current index
        $this->removeAllProcessingIndex();

        return $num;
    }

    /**
     * status
     */
    public function status(): array
    {
        $ret = [];
        $ret['total pending index'] = $this->redis->lLen($this->getIndexListName());
        $ret['total blocked index'] = $this->redis->lLen($this->getBlockedListName());
        return $ret;
    }
}
