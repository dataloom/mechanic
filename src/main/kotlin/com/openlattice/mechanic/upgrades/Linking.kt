/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import org.slf4j.LoggerFactory
import java.util.*


/**
 *
 * Migrations for linking.
 */
class Linking(private val toolbox: Toolbox) : Upgrade {
    companion object {
        private val logger = LoggerFactory.getLogger(Linking::class.java)
    }

    override fun upgrade(): Boolean {
        return migrateIdsTable()
                && upgradePropertyTypes()
                && migrateEntitySetTables() // && upgradEntitySets
    }

    override fun getSupportedVersion(): Long {
        return Version.V2018_09_14.value
    }

    private fun migrateIdsTable(): Boolean {
        logger.info("Adding new columns to ${IDS.name}.")
        //First we add the new columns to the table.
        alterIdsTable().map { sql ->
            toolbox.executor.submit {
                toolbox.hds.connection.use {
                    it.createStatement().use {
                        logger.info("Executing: $sql")
                        it.execute(sql)
                    }
                }
            }
        }.forEach { it.get() }

        logger.info("Adding indexes to ${IDS.name}.")
        //Next we add indexes
        addIdsIndexes().map { sql ->
            toolbox.executor.submit {
                toolbox.hds.connection.use {
                    it.createStatement().use {
                        it.execute(sql)
                    }
                }
            }
        }.forEach { it.get() }

        logger.info("Adding constraints to ${IDS.name}.")
        //Now we set default values and alter constraints
        setConstraintsForIdsTable().map { sql ->
            toolbox.executor.submit {
                toolbox.hds.connection.use {
                    it.createStatement().use {
                        logger.info("Executing $sql")
                        it.execute(sql)
                    }
                }
            }
        }.forEach { it.get() }

        return true
    }

    private fun migrateEntitySetTables(): Boolean {
        toolbox.entitySets.map { es ->
            toolbox.executor.submit {
                toolbox.hds.connection.use {
                    it.use {
                        it.createStatement().use {
                            it.execute(updateIdsTable(es.key))
                            logger.info("Migrated entity key ids table {}", es.key)
                        }
                    }
                }
            }
        }.forEach { it.get() }

        return true
    }

    private fun upgradePropertyTypes(): Boolean {
        toolbox.propertyTypes.map { pt ->
            toolbox.executor.submit {
                toolbox.hds.connection.use {
                    it.use {
                        it.createStatement().use {
                            it.execute(dropClusterId(pt.key))
                            logger.info("Processed property type {}", pt.key)
                        }
                    }
                }
            }
        }.forEach {
            it.get()
        }
        return true
    }

    private fun upgradeEntitySets(): Boolean {
        val connection = toolbox.hds.connection
        connection.use {
            it.createStatement().use { stmt ->
                toolbox.entitySets.forEach {
                    stmt.execute(addLastLinkedColumn(it.key))
                }
            }
        }
        return true
    }

    private fun dropClusterId(propertyTypeId: UUID): String {
        val propertyTableName = quote(propertyTableName(propertyTypeId))
        //TODO: If drop last write column we have to update insert statements
        return "ALTER TABLE $propertyTableName DROP COLUMN IF EXISTS cluster_id; "// +
        //"ALTER TABLE $propertyTableName DROP COLUMN IF EXISTS LAST_WRITE; "
    }

    private fun addClusterId(propertyTypeId: UUID): String {
        val propertyTableName = propertyTableName(propertyTypeId)

        val valueColumn = value(toolbox.propertyTypes[propertyTypeId]!!)
        val ptd = PostgresTableDefinition(quote(propertyTableName))
                .addColumns(
                        LINKING_ID,
                        ENTITY_SET_ID,
                        ID_VALUE,
                        HASH,
                        valueColumn,
                        VERSION,
                        VERSIONS,
                        LAST_WRITE,
                        READERS,
                        WRITERS,
                        OWNERS
                )
                .primaryKey(ENTITY_SET_ID, ID_VALUE, HASH)

        val clusterIndex = PostgresColumnsIndexDefinition(ptd, LINKING_ID)
                .name(quote("${propertyTableName}_cluster_idx"))
                .ifNotExists()

        return "ALTER TABLE ${quote(propertyTableName)} " +
                "   ADD COLUMN IF NOT EXISTS ${LINKING_ID.name} ${PostgresDatatype.UUID.sql()}; " +
                "${clusterIndex.sql()}; "
    }
}

private fun updateIdsTable(entitySetId: UUID): String {
    val entitySetTableName = entityTableName(entitySetId)
    val setSql = listOf(VERSION.name, VERSIONS.name, LAST_WRITE.name, LAST_INDEX.name, LAST_LINK.name)
            .joinToString(",") { "$it = $entitySetTableName.$it" }

    return "UPDATE ${IDS.name} " +
            "SET $setSql " +
            "FROM $entitySetTableName " +
            "WHERE ${IDS.name}.${ENTITY_SET_ID.name} = $entitySetTableName.${ENTITY_SET_ID.name} " +
            "   AND ${IDS.name}.${ID.name} = $entitySetTableName.${ID_VALUE.name} "
}

private fun addIdsIndexes(): List<String> {
    return listOf(
            PostgresExpressionIndexDefinition(IDS, "(${LAST_INDEX.name} < ${LAST_WRITE.name})")
                    .name("entity_key_ids_needs_indexing_idx")
                    .ifNotExists(),
            PostgresExpressionIndexDefinition(IDS, "(${LAST_LINK.name} < ${LAST_WRITE.name})")
                    .name("entity_key_ids_needs_linking_idx")
                    .ifNotExists(),
            PostgresExpressionIndexDefinition(IDS, "(${LAST_PROPAGATE.name} < ${LAST_WRITE.name})")
                    .name("entity_key_ids_needs_propagation_idx")
                    .ifNotExists(),
            PostgresColumnsIndexDefinition(IDS, VERSION)
                    .name("entity_key_ids_version_idx")
                    .ifNotExists(),
            PostgresColumnsIndexDefinition(IDS, VERSIONS)
                    .name("entity_key_ids_versions_idx")
                    .method(IndexMethod.GIN)
                    .ifNotExists(),
            PostgresColumnsIndexDefinition(IDS, LINKING_ID)
                    .name("entity_key_ids_linking_id_idx")
                    .ifNotExists(),
            PostgresColumnsIndexDefinition(IDS, LAST_WRITE)
                    .name("entity_key_ids_last_write_idx")
                    .ifNotExists(),
            PostgresColumnsIndexDefinition(IDS, LAST_INDEX)
                    .name("entity_key_ids_last_index_idx")
                    .ifNotExists(),
            PostgresColumnsIndexDefinition(IDS, LAST_PROPAGATE)
                    .name("entity_key_ids_last_propagate_idx")
                    .ifNotExists()
    ).map(PostgresIndexDefinition::sql)
}

private fun alterIdsTable(): List<String> {
    return listOf(VERSION, VERSIONS, LAST_WRITE, LAST_INDEX, LAST_LINK, LAST_PROPAGATE, LINKING_ID)
            .map {
                "ALTER TABLE ${IDS.name} ADD COLUMN IF NOT EXISTS ${it.name} ${it.datatype.sql()}"
            }

}

private fun setConstraintsForIdsTable(): List<String> {
    return listOf(VERSION, VERSIONS, LAST_WRITE, LAST_INDEX, LAST_LINK, LAST_PROPAGATE, LINKING_ID)
            .map {
                val defaultValue = when (it) {
                    VERSION -> "-1 "
                    VERSIONS -> "ARRAY[-1] "
                    else -> "'-infinity'"

                }
                "UPDATE ${IDS.name} SET ${it.name} = $defaultValue WHERE ${it.name} IS NULL;\n" +
                        "ALTER TABLE ${IDS.name} ALTER COLUMN ${it.name} SET NOT NULL;\n" +
                        "ALTER TABLE ${IDS.name} ALTER COLUMN ${it.name} SET DEFAULT $defaultValue;"
            }

}

private fun addLastLinkedColumn(entitySetId: UUID): String {
    val entitySetTableName = entityTableName(entitySetId)
    return "ALTER TABLE ${quote(entitySetTableName)} " +
            "   ADD COLUMN IF NOT EXISTS ${LAST_LINK.name} ${PostgresDatatype.TIMESTAMPTZ.sql()}; " +
            "UPDATE ${quote(entitySetTableName)} SET ${LAST_LINK.name} = '-infinity';" +
            "ALTER TABLE ${quote(entitySetTableName)} ALTER COLUMN ${LAST_LINK.name} SET NOT NULL; " +
            "ALTER TABLE ${quote(entitySetTableName)} ALTER COLUMN ${LAST_LINK.name} SET DEFAULT '-infinity'; "
}