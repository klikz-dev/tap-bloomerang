<?php

use GuzzleHttp\Client;
use SingerPhp\SingerTap;
use SingerPhp\Singer;

class BloomerangTap extends SingerTap
{
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * The base URL of the Bloomerang REST API
     * @var string
     */
    const BASE_API_URL = 'https://api.bloomerang.co/v2/';

    /**
     * Maximum number of API retries
     * @var integer
     */
    const RETRY_LIMIT = 5;

    /**
     * Delay of retry cycle (seconds)
     * @var integer
     */
    const RETRY_DELAY = 30;

    /**
     * Records per page
     * @var integer
     */
    const RECORDS_PER_PAGE = 50;

    /**
     * Private Key
     * @var string
     */
    private $private_key = '';

    public function test()
    {
        $this->private_key = $this->singer->config->input('private_key');
        try {
            $results = $this->request("addresses");
            $this->singer->writeMeta(['test_result' => true]);
        } catch (Exception $e) {
            $this->singer->writeMeta(['test_result' => false]);
        }
    }

    public function discover()
    {
        $this->singer->logger->debug('Starting discover for tap Bloomerang');

        $this->private_key = $this->singer->config->setting('private_key');

        foreach ($this->singer->config->catalog->streams as $stream) {
            $table = $stream->stream;
            $this->singer->logger->debug("Writing schema for {$table}");

            $columns = $this->getTableColumns($table);

            $key_properties = [];
            if ( array_key_exists('Id', $columns) ) {
                $this->singer->writeMeta(['unique_keys' => ['Id']]);
                $key_properties = ['Id'];
            }

            $this->singer->writeSchema(
                stream: $table,
                schema: $columns,
                key_properties: $key_properties
            );
        }
    }

    public function tap()
    {
        $this->singer->logger->debug('Starting sync for tap Bloomerang');
        $this->singer->logger->debug("catalog", [$this->singer->config->catalog]);

        $this->private_key = $this->singer->config->setting('private_key');

        foreach ($this->singer->config->catalog->streams as $stream) {
            $table = $stream->stream;
            $columns = $this->getTableColumns($table);

            $this->singer->logger->debug("Writing schema for {$table}");

            $key_properties = [];
            if ( array_key_exists('Id', $columns) ) {
                $this->singer->writeMeta(['unique_keys' => ['Id']]);
                $key_properties = ['Id'];
            }

            $this->singer->writeSchema(
                stream: $table,
                schema: $columns,
                key_properties: $key_properties
            );

            $this->singer->logger->debug("Starting sync for {$table}");

            $uri =      $this->table_map[$table]['path'];
            $paginate = $this->table_map[$table]['paginate'];

            $current_page = 1;
            $total_records = 0;
            while (true) {
                $skip = self::RECORDS_PER_PAGE * ($current_page - 1);

                $response = $this->requestWithRetries("{$uri}?skip={$skip}&take=" . self::RECORDS_PER_PAGE);

                if ($paginate) {
                    $results = $response['Results'];
                } else {
                    $results = $response;
                }

                $records = [];
                $ids     = [];
                foreach ($results as $result) {
                    $record = $this->formatRecord((array) $result, $columns);

                    $records[] = $record;
                    $ids[]     = $record['Id'];
                }

                $this->_deleteRecords($table, $ids);
                $this->_insertRecords($table, $records);

                $total_records += count($records);

                if ( ! $paginate || (int) $response['ResultCount'] < 50) {
                    break;
                }
                $current_page += 1;
            }

            $this->singer->writeMetric(
                'counter',
                'record_count',
                $total_records,
                [
                    'table' => $table
                ]
            );

            $this->singer->logger->debug("Finished sync for {$table}");
        }
    }

    public function getTables()
    {
        $tables = array_values(array_keys($this->table_map));
        $this->singer->writeMeta(compact('tables'));
    }

    /**
     * Generate table column array
     * @param  string   $table  The table name
     * @return array    structured column array ready for signer schema
     */
    public function getTableColumns($table)
    {
        $uri = $this->table_map[$table]['path'];
        $paginate = $this->table_map[$table]['paginate'];
        $response = $this->request("{$uri}?skip=0&take=1");

        if ($paginate) {
            $results = $response['Results'];
        } else {
            $results = $response;
        }

        $columns = [];
        if ( isset($results[0]) ) {
            foreach ($results[0] as $column_name => $column_value) {
                $columns[$column_name] = [
                    'type' => $this->getColumnType($column_value)
                ];
            }
        }

        return $columns;
    }

    /**
     * Attempt to determine the data type of a column based on its value
     *
     * @param  mixed  $value The value of the column
     * @return string The postgres friendly data type to be used in the column definition
     */
    public function getColumnType($value): string
    {
        $lookup = [
            'array'   => Singer::TYPE_ARRAY,
            'boolean' => Singer::TYPE_BOOLEAN,
            'integer' => Singer::TYPE_INTEGER,
            'float'   => Singer::TYPE_FLOAT,
            'null'    => Singer::TYPE_STRING,
            'object'  => SINGER::TYPE_OBJECT,
            'string'  => SINGER::TYPE_STRING,
            'timestamp'  => SINGER::TYPE_TIMESTAMP,
            'timestamp-tz'  => SINGER::TYPE_TIMESTAMPTZ,
        ];

        $type = strtolower(gettype($value));

        return array_key_exists($type, $lookup) ? $lookup[$type] : Singer::TYPE_STRING;
    }

    /**
     * Make a request with retry logic
     * @param string    $uri   The API URI
     * @return array    The API response array
     */
    public function requestWithRetries($uri)
    {
        $attempts = 1;
        while (true) {
            try {
                return $this->request($uri);
            } catch (Exception $e) {
                if ($attempts > self::RETRY_LIMIT) {
                    throw $e;
                }
                $this->singer->logger->debug("Bloomerang request failed. Retrying. Attempt {$attempts} of " . self::RETRY_LIMIT . " in " . self::RETRY_DELAY . " seconds.");
                $attempts++;
                sleep(self::RETRY_DELAY);
            }
        }
    }

    /**
     * Make a request to the Bloomerang API
     * @param  string   $uri    The API URI
     * @return array    The API response array
     */
    public function request($uri)
    {
        $client = new Client([
            'base_uri' => self::BASE_API_URL,
            'headers'  => [
                "X-API-KEY" => $this->private_key
            ],
            'http_errors' => false
        ]);
        $response = $client->get($uri);

        $status_code = $response->getStatusCode();
        switch ($status_code) {
            case 200:
                return (array) json_decode((string) $response->getBody());
            case 401:
                throw new Exception('Bloomerang credentials were invalid.');
            default:
                throw new Exception("Resource not found or another HTTP error occurred. code: {$status_code}");
        }
    }

    /**
     * Format records to match table columns
     *
     * @param array   $record           The response array
     * @param array   $columns          The record model
     *
     * @return array
     */
    public function formatRecord($record, $columns) {
        // Remove unmapped fields from the response.
        $record = array_filter($record, function($key) use($columns) {
            return array_key_exists($key, $columns);
        }, ARRAY_FILTER_USE_KEY);

        // column mapping for missing response fields.
        foreach ($columns as $colKey => $colVal) {
            if (!array_key_exists($colKey, $record)) {
                $record[$colKey] = null;
            }
        }

        return $record;
    }

    /**
     * Handles deleting duplicated records from the database.
     *
     * @param string  $table    The table name
     * @param array   $ids      Array of record ids to delete
     *
     * @return void
     */
    function _deleteRecords($table, $ids)
    {
        foreach ($ids as $id) {
            $this->singer->writeDeleteRecord(
                stream: $table,
                record: ['Id' => $id],
                soft_delete: true,
            );
        }
    }

    /**
     * Handles inserting records into the database.
     *
     * @param string  $table    The table name
     * @param array   $records  Array of records to insert
     *
     * @return void
     */
    function _insertRecords($table, $records)
    {
        foreach ($records as $record) {
            $this->singer->writeRecord(
                stream: $table,
                record: $record
            );
        }
    }

    /**
     * Array of table data.
     *
     * @var array
     */
    private $table_map = [
        'addresses' => [
            'path' => 'addresses',
            'paginate' => true,
        ],
        'appeals' => [
            'path' => 'appeals',
            'paginate' => true,
        ],
        'campaigns' => [
            'path' => 'campaigns',
            'paginate' => true,
        ],
        'constituents' => [
            'path' => 'constituents',
            'paginate' => true,
        ],
        'customfields_constituent' => [
            'path' => 'customfields/Constituent',
            'paginate' => false,
        ],
        'customfields_transaction' => [
            'path' => 'customfields/Transaction',
            'paginate' => false,
        ],
        'customfields_interaction' => [
            'path' => 'customfields/Interaction',
            'paginate' => false,
        ],
        'customfields_note' => [
            'path' => 'customfields/Note',
            'paginate' => false,
        ],
        'customfields_benevon' => [
            'path' => 'customfields/Benevon',
            'paginate' => false,
        ],
        'customvalues_constituent' => [
            'path' => 'customvalues/Constituent',
            'paginate' => false,
        ],
        'customvalues_transaction' => [
            'path' => 'customvalues/Transaction',
            'paginate' => false,
        ],
        'customvalues_interaction' => [
            'path' => 'customvalues/Interaction',
            'paginate' => false,
        ],
        'customvalues_note' => [
            'path' => 'customvalues/Note',
            'paginate' => false,
        ],
        'customvalues_benevon' => [
            'path' => 'customvalues/Benevon',
            'paginate' => false,
        ],
        'customfieldcategories_constituent' => [
            'path' => 'customfieldcategories/Constituent',
            'paginate' => false,
        ],
        'customfieldcategories_transaction' => [
            'path' => 'customfieldcategories/Transaction',
            'paginate' => false,
        ],
        'customfieldcategories_interaction' => [
            'path' => 'customfieldcategories/Interaction',
            'paginate' => false,
        ],
        'customfieldcategories_note' => [
            'path' => 'customfieldcategories/Note',
            'paginate' => false,
        ],
        'customfieldcategories_benevon' => [
            'path' => 'customfieldcategories/Benevon',
            'paginate' => false,
        ],
        'emails' => [
            'path' => 'emails',
            'paginate' => true,
        ],
        'emailinterests' => [
            'path' => 'emailinterests',
            'paginate' => true,
        ],
        'funds' => [
            'path' => 'funds',
            'paginate' => true,
        ],
        'households' => [
            'path' => 'households',
            'paginate' => true,
        ],
        'interactions' => [
            'path' => 'interactions',
            'paginate' => true,
        ],
        'notes' => [
            'path' => 'notes',
            'paginate' => true,
        ],
        'phones' => [
            'path' => 'phones',
            'paginate' => true,
        ],
        'processors' => [
            'path' => 'processors',
            'paginate' => true,
        ],
        'refunds' => [
            'path' => 'refunds',
            'paginate' => true,
        ],
        'relationshiproles' => [
            'path' => 'relationshiproles',
            'paginate' => true,
        ],
        'softcredits' => [
            'path' => 'softcredits',
            'paginate' => true,
        ],
        'tasks' => [
            'path' => 'tasks',
            'paginate' => true,
        ],
        'transactions' => [
            'path' => 'transactions',
            'paginate' => true,
        ],
        'tributes' => [
            'path' => 'tributes',
            'paginate' => true,
        ],
        'walletitems' => [
            'path' => 'walletitems',
            'paginate' => true,
        ]
    ];
}
