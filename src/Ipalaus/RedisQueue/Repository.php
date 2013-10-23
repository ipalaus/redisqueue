<?php namespace Ipalaus\RedisQueue;

use Str;
use UnexpectedValueException;
use Illuminate\Redis\Database;
use Illuminate\Support\Collection;

class Repository {

	/**
	 * Redis connection.
	 *
	 * @var \Illuminate\Redis\Database
	 */
	protected $database;

	/**
	 * Default namespace where our queues are grouped.
	 *
	 * @var string
	 */
	protected $namespace = 'queues';

	/**
	 * Create a new RedisQueue repository.
	 *
	 * @param  \Illuminate\Redis\Database $database
	 * @return void
	 */
	public function __construct(Database $database)
	{
		$this->database = $database;
	}

	/**
	 * Fetch all the queues inside the 'queues' namespace with the delayed and
	 * reserved.
	 *
	 * @return \Illuminate\Support\Collection
	 */
	public function getAll()
	{
		$collection = new Collection;

		$keys = $this->database->keys($this->namespace.':*');

		foreach ($keys as $key)
		{
			list($namespace, $name) = explode(':', $key);

			if ( ! $collection->has($name))
			{
				$collection->put($name, $this->get($name));
			}
		}

		return $collection;
	}

	/**
	 * Get the items that a concrete queue contain.
	 *
	 * @param  string  $name
	 * @return array
	 */
	public function get($name)
	{
		$namespaced = $this->namespace.':'.$name;

		$queued   = $this->length($namespaced, 'list');
		$delayed  = $this->length($namespaced.':delayed', 'zset');
		$reserved = $this->length($namespaced.':reserved', 'zset');

		return array('queued' => $queued, 'delayed' => $delayed, 'reserved' => $reserved);
	}

	/**
	 * Retruns a list of items for a given key.
	 *
	 * @param  string $name
	 * @param  string $type
	 * @return \Illuminate\Support\Collection
	 */
	public function getItems($name, $type)
	{
		$namespaced = $this->namespace.':'.$name;

		$key = ($type === 'queued') ? $namespaced : $namespaced.':'.$type;

		if ( ! $this->exists($key)) throw new QueueNotFoundException($key);

		$type = $this->type($key);
		$length = $this->length($key, $type);

		$method = 'get'.ucwords($type).'Items';
		$items = $this->{$method}($key, $length);

		return Collection::make($items);
	}

	/**
	 * Returns a list of items inside a list.
	 *
	 * @param  string   $key
	 * @param  integer  $total
	 * @param  integer  $offset
	 * @return array
	 */
	protected function getListItems($key, $total, $offset = 0)
	{
		$items = array();

		for ($i = $offset; $i < $total; $i++) {
			$items[$i] = $this->database->lindex($key, $i);
		}

		return $items;
	}

	/**
	 * Returns a list of items inside a zset.
	 *
	 * @param  string   $key
	 * @param  integer  $total
	 * @param  integer  $offset
	 * @return array
	 */
	protected function getZsetItems($key, $total, $offset = 0)
	{
		return $this->database->zrange($key, 0, -1);
	}

	/**
	 * Checks if a given key exists.
	 * @param  string  $key
	 * @return bool
	 */
	public function exists($key)
	{
		return (bool) $this->database->exists($key);
	}

	/**
	 * Returns the type of a Redis key.
	 *
	 * @param  string  $key
	 * @return string
	 */
	public function type($key)
	{
		if (Str::endsWith($key, ':queued')) {
			$key = str_replace(':queued', '', $key);
		}

		return $this->database->type($key);
	}

	/**
	 * Return the length of a given list or set.
	 *
	 * @param  string  $key
	 * @param  string  $type
	 * @return integer|\UnexpectedValueException
	 */
	public function length($key, $type = null)
	{
		if (is_null($type)) $type = $this->type($key);

		switch ($type)
		{
			case 'list':
				return $this->database->llen($key);

			case 'zset':
				return $this->database->zcard($key);

			default:
				throw new UnexpectedValueException("List type '{$type}' not supported.");
		}
	}

	/**
	 * Remove an item with a given key, index or value.
	 *
	 * @param  string  $key
	 * @param  string  $value
	 * @param  string  $type
	 * @return bool
	 */
	public function remove($key, $value, $type = null)
	{
		if (is_null($type)) $type = $this->type($key);

		switch ($type)
		{
			case 'list':
				$key    = str_replace(':queued', '', $key);
				$random = Str::quickRandom(64);

				$this->database->lset($key, $value, $random);

				return $this->database->lrem($key, 1, $random);

			case 'zset':
				return $this->database->zrem($key, $value);

			default:
				throw new UnexpectedValueException("Unable to delete {$value} from {$key}. List type {$type} not supported.");
		}
	}

}