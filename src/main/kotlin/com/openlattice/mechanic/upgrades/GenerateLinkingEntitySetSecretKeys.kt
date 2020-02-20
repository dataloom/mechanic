/*
 * Copyright (C) 2019. OpenLattice, Inc.
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

import com.openlattice.ids.IdCipherManager
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresTable
import org.slf4j.LoggerFactory

class GenerateLinkingEntitySetSecretKeys(
        private val toolbox: Toolbox,
        private val cipherManager: IdCipherManager
) : Upgrade {
    companion object {
        private val logger = LoggerFactory.getLogger(GenerateLinkingEntitySetSecretKeys::class.java)
    }

    override fun upgrade(): Boolean {
        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                logger.info("Creating the ${PostgresTable.LINKED_ENTITY_SET_SECRET_KEYS.name} table.")
                stmt.execute(PostgresTable.LINKED_ENTITY_SET_SECRET_KEYS.createTableQuery())
            }
        }

        toolbox.entitySets.filter { it.value.isLinking }.forEach { (entitySetId, _) ->
            logger.info("Generating secret key for $entitySetId.")
            cipherManager.assignSecretKey(entitySetId)
        }

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_01_03.value
    }
}