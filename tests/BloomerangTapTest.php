<?php

declare(strict_types=1);

namespace Tests;

define('TAP_BASE_PATH', __DIR__);

use PHPUnit\Framework\TestCase;
use Exception;
use SingerPhp\SingerParser;
use SingerPhp\Messages\MetaMessage;
use SingerPhp\Messages\RecordMessage;
use SingerPhp\Messages\SchemaMessage;
use SingerPhp\Messages\StateMessage;
use SingerPhp\Configuration;
use BloomerangTap;

class BloomerangTapTest extends TestCase
{
    public function testHasDesiredMethods()
    {
        $this->assertTrue(method_exists('BloomerangTap', 'test'));
        $this->assertTrue(method_exists('BloomerangTap', 'discover'));
        $this->assertTrue(method_exists('BloomerangTap', 'tap'));
        $this->assertTrue(method_exists('BloomerangTap', 'getTables'));
    }

    public function testConfiguration()
    {
        ob_start();
        (new \BloomerangTap())->getTables();
        $data = ob_end_clean();

        $this->assertEquals(
            '{"type":"META","metadata":{"tables":["addresses","appeals","campaigns","constituents","customfields_constituent","customfields_transaction","customfields_interaction","customfields_note","customfields_benevon","customvalues_constituent","customvalues_transaction","customvalues_interaction","customvalues_note","customvalues_benevon","customfieldcategories_constituent","customfieldcategories_transaction","customfieldcategories_interaction","customfieldcategories_note","customfieldcategories_benevon","emails","funds","households","interactions","notes","phones","processors","refunds","relationshiproles","softcredits","tasks","transactions","tributes","walletitems"]}',
            $data
        );
    }

    /**
     * @covers \BloomerangModel::getColumnType
     */
    public function testGetColumnType()
    {
        $tap = new \BloomerangTap();

        $this->assertEquals(
            'string',
            $tap->getColumnType('this should be varchar')
        );

        $this->assertEquals(
            'string',
            $tap->getColumnType(NULL)
        );

        $this->assertEquals(
            'integer',
            $tap->getColumnType(5)
        );

        $this->assertEquals(
            'boolean',
            $tap->getColumnType(TRUE)
        );

        $this->assertEquals(
            'json',
            $tap->getColumnType(['a' => 'b'])
        );
    }
}
