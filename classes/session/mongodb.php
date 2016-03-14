<?php defined('SYSPATH') OR die('No direct script access.');

/**
 * MongoDB-based session class.
 *
 * @category   Session
 * @author     Vladimir Zolotoy <vovazolotoy@gmail.com>
 * @license    MIT
 */
class Session_Mongodb extends Session {

    // Mongo host
    protected $_host = 'localhost';

    // Mongo port
    protected $_port = 27017;

    // Database name
    protected $_db = 'sessions';

    // Collection name
    protected $_collection = 'sessions';

    // Collection field names
    protected $_columns = array(
        'session_id'  => 'session_id',
        'last_active' => 'last_active',
        'contents'    => 'contents'
    );

    // Garbage collection requests
    protected $_gc = 500;

    // The current session id
    protected $_session_id;

    // The old session id
    protected $_update_id;

    public function __construct(array $config = NULL, $id = NULL)
    {
        if (isset($config['host']))
        {
            // Set the mongo host
            $this->_host = (string) $config['host'];
        }

        if (isset($config['port']))
        {
            // Set the mongo port
            $this->_port = (int) $config['port'];
        }

        if (isset($config['user']))
        {
            $this->_user = $config['user'];
        }

        if (isset($config['password']))
        {
            $this->_password = $config['password'];
        }

        if (isset($config['database']))
        {
            // Set the database name
            $this->_database = (string) $config['database'];
        }

        if (isset($config['collection']))
        {
            // Set the collection name
            $this->_collection = (string) $config['collection'];
        }

        if (isset($config['gc']))
        {
            // Set the gc chance
            $this->_gc = (int) $config['gc'];
        }

        if (isset($config['columns']))
        {
            // Overload column names
            $this->_columns = $config['columns'];
        }

        if ( ! class_exists('MongoDB\Client'))
        {
            throw new Kohana_Exception('Mongo class is not exists!');
        }

        // Connect to Mongo server

        $mongo = new MongoDB\Client('mongodb://'.$this->_host . ':' . $this->_port);

        // Select database and collection
        $this->_db = $mongo->selectDatabase($this->_db);

        $this->_collection = $this->_db->selectCollection($this->_collection);

        parent::__construct($config, $id);

        if (mt_rand(0, $this->_gc) === $this->_gc)
        {
            // Run garbage collection
            // This will average out to run once every X requests
            $this->_gc();
        }

    }

    public function id()
    {
        return $this->_session_id;
    }

    protected function _read($id = NULL)
    {
        if ($id OR $id = Cookie::get($this->_name, NULL, FALSE))
        {
            $object = $this->_collection
                ->findOne(array($this->_columns['session_id'] => $id));

            if (count($object))
            {
                // Set the current session id
                $this->_session_id = $this->_update_id = $id;
                // Return the contents
                return $object[$this->_columns['contents']];
            }
        }

        // Create a new session id
        $this->_regenerate();

        return NULL;
    }

    protected function _regenerate()
    {
        do
        {
            // Create a new session id
            $id = str_replace('.', '-', uniqid(NULL, TRUE));

            // Get the the id from the database
            $object = $this->_collection
                ->findOne(array($this->_columns['session_id'] => $id));
        }
        while (count($object));

        return $this->_session_id = $id;
    }

    protected function _write()
    {
        $object = array(
            $this->_columns['session_id']  => $this->_session_id,
            $this->_columns['last_active'] => $this->_data['last_active'],
            $this->_columns['contents']    => $this->__toString()
        );

        if ($this->_update_id === NULL)
        {
            // Insert a new object
            if ( ! $this->_collection->insertOne($object))
            {
                throw new Kohana_Exception('Cannot create new session record (:error)', array(':error' => $this->_db->lastError()));
            }
        }
        else
        {
            if ($this->_update_id !== $this->_session_id)
            {
                // Also update the session id
                $object[$this->_columns['session_id']] = $this->_session_id;
            }

            // Update the row
            if ( ! $this->_collection->replaceOne(array($this->_columns['session_id'] => $this->_update_id), $object))
            {
                throw new Kohana_Exception('Cannot update session record (:error)', array(':error' => $this->_db->lastError()));
            }
        }

        // The update and the session id are now the same
        $this->_update_id = $this->_session_id;
        // Update the cookie with the new session id
        Cookie::set($this->_name, $this->_session_id, $this->_lifetime, FALSE);

        return TRUE;
    }

    protected function _destroy()
    {
        if ($this->_update_id === NULL)
        {
            // Session has not been created yet
            return TRUE;
        }

        // Delete the current session
        if ($this->_collection->deleteOne(array($this->_columns['session_id'] => $this->_update_id)))
        {
            // Delete the cookie
            Cookie::delete($this->_name);
        }
        else
        {
            // An error occurred, the session has not been deleted
            throw new Kohana_Exception('Cannot destroy session (:error)', array(':error' => $this->_db->lastError()));
        }

        return TRUE;
    }

    protected function _gc()
    {
        if ($this->_lifetime)
        {
            // Expire sessions when their lifetime is up
            $expires = $this->_lifetime;
        }
        else
        {
            // Expire sessions after one month
            $expires = Date::MONTH;
        }

        $expired = __('this.:column < :time', array(':column' => $this->_columns['last_active'], ':time' => (time() - $expires)));

        if ( ! $this->_collection->deleteMany(array('$where' => $expired)))
        {
            throw new Kohana_Exception('Cannot delete old sessions (:error)', array(':error' => $this->_db->lastError()));
        }
    }

    protected function _restart()
    {
        throw new Exception('Session driver MongoDB restart does not supported.');
    }

} // End Session_Mongodb
