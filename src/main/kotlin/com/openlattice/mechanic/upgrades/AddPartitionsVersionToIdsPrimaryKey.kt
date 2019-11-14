package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable
import org.slf4j.LoggerFactory

class AddPartitionsVersionToIdsPrimaryKey(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(AddOriginIdToDataPrimaryKey::class.java)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_10_03.value
    }

    override fun upgrade(): Boolean {

        val pkey = (PostgresTable.IDS.primaryKey + PostgresColumn.PARTITIONS_VERSION).joinToString(",") { it.name }

        logger.info("Adding new index to ids to be used for primary key.")

        val createNewPkeyIndex = "CREATE UNIQUE INDEX CONCURRENTLY ${PostgresTable.IDS.name}_pkey_idx ON ${PostgresTable.IDS.name} ($pkey)"

        toolbox.hds.connection.use { conn ->
            conn.createStatement().execute( createNewPkeyIndex )
        }

        logger.info("Finished creating index. About to drop pkey constraint and create new constraint using new index.")

        val dropPkey = "ALTER TABLE ${PostgresTable.IDS.name} DROP CONSTRAINT ${PostgresTable.IDS.name}_pkey"
        val updatePkey = "ALTER TABLE ${PostgresTable.IDS.name} ADD CONSTRAINT ${PostgresTable.IDS.name}_pkey PRIMARY KEY USING INDEX ${PostgresTable.IDS.name}_pkey_idx"

        logger.info("About to drop and recreate primary key of data.")

        toolbox.hds.connection.use { conn ->
            conn.autoCommit = false
            conn.createStatement().executeUpdate( dropPkey )
            conn.createStatement().executeUpdate( updatePkey )
            conn.commit()
        }

        logger.info("Finished updating primary key.")

        return true
    }
}