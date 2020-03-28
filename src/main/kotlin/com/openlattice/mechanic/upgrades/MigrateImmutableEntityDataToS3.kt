package com.openlattice.mechanic.upgrades

import com.google.common.base.Stopwatch
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.elasticsearch.common.util.set.Sets
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

class MigrateImmutableEntityDataToS3(
        private val toolbox: Toolbox,
        private val byteBlobDataManager: ByteBlobDataManager
) : Upgrade {
    private val limiter = Semaphore(16)

    override fun getSupportedVersion(): Long {
        return Version.V2020_03_25.value
    }

    companion object {
        private val logger = LoggerFactory.getLogger(MigrateImmutableEntityDataToS3::class.java)

        val LAST_MIGRATE = "last_migrate"
        val BATCH_SIZE = 16_000

        val LAST_VALID_MIGRATE = "2020-03-26T22:36:34.64+00:00"
    }


    override fun upgrade(): Boolean {

        val partitionManager = PartitionManager(toolbox.hazelcast, toolbox.hds)
        val pedqs = PostgresEntityDataQueryService(toolbox.hds, byteBlobDataManager, partitionManager)

        val immutableEntitySets = getImmutableEntitySets()

        // read immutable entities out of data table and write them into S3
        writeImmmutableDataToS3(pedqs, immutableEntitySets)

        // delete migrated immutable rows from the data table
        deleteMigratedData(immutableEntitySets)

        return true
    }

    private fun getImmutableEntitySets(): Collection<EntitySet> {
        return toolbox.entitySets.values
                .filter {
                    !it.flags.contains(EntitySetFlag.AUDIT)
                            && !it.isLinking
                            && true /* TODO filter to immutable  */
                }
    }


    private fun writeImmmutableDataToS3(pedqs: PostgresEntityDataQueryService, immutableEntitySets: Collection<EntitySet>) {
        logger.info("About to write immutable entities to S3")
        val sw = Stopwatch.createStarted()

        val propertyTypesByEntityTypeId = toolbox.entityTypes.values.associate {
            it.id to it.properties.associateWith { ptId -> toolbox.propertyTypes.getValue(ptId) }
        }

        immutableEntitySets.stream().parallel().forEach { entitySet ->
            limiter.acquire()

            logger.info("About to migrate entity set ${entitySet.name}")

            val entitySetPropertyTypes = mapOf(
                    entitySet.id to propertyTypesByEntityTypeId.getValue(entitySet.entityTypeId)
            )

            entitySet.partitions.forEach { partition ->
                val getIdsBatchSql = getBatchOfIdsSql(entitySet.id, partition)
                logger.info("Migrating data for entity set ${entitySet.name} on partition $partition using SQL: $getIdsBatchSql")

                val partitionSw = Stopwatch.createStarted()
                var partitionTotalUpdated = 0
                var insertCount = 1

                while (insertCount > 0) {

                    // 1: Load batch of ids to migrate
                    val idsBatch = BasePostgresIterable(StatementHolderSupplier(toolbox.hds, getIdsBatchSql)) {
                        ResultSetAdapters.id(it)
                    }.toSet()

                    if (idsBatch.isNotEmpty()) {

                        // 2: Load entities of id batch
                        val entities = pedqs.getEntitiesWithPropertyTypeIds(
                                mapOf(entitySet.id to Optional.of(idsBatch)),
                                entitySetPropertyTypes
                        ).toMap() // TODO fix binary data values

                        // 3: Write those entities to S3
                        // TODO insert to s3 (getBatchOfEntities)

                        // 4: Update last_migrate on id batch
                        toolbox.hds.connection.use { conn ->
                            conn.createStatement().execute(updateIdsLastMigrateSql(entitySet.id, partition, idsBatch))
                        }

                    }

                    insertCount = idsBatch.size

                    partitionTotalUpdated += insertCount
                    logger.info("Migrated a batch of $insertCount entities for partition $partition of entity set ${entitySet.name}")

                }

                logger.info("Finished migrating $partitionTotalUpdated rows for partition $partition in ${partitionSw.elapsed(TimeUnit.MILLISECONDS)} ms.")
                limiter.release()
            }

            logger.info("Finished migrating entity set ${entitySet.name}")

        }

        logger.info("All the immutable data has been migrated from the data table to S3!")
    }

    private fun deleteMigratedData(immutableEntitySets: Collection<EntitySet>) {
        val deleteSql = deleteSql(immutableEntitySets)
        logger.info("About to delete migrated immutable data from the data table using sql: $deleteSql")
        val sw = Stopwatch.createStarted()

        toolbox.hds.connection.use { conn ->
            conn.createStatement().execute(deleteSql)
        }

        logger.info("Finished deleting migrated immutable data. Took ${sw.elapsed(TimeUnit.SECONDS)} seconds.")

    }

    private fun getBatchOfIdsSql(entitySetId: UUID, partition: Int): String {
        return "SELECT ${ID.name} " +
                "FROM ${IDS.name} " +
                "WHERE ${PARTITION.name} = $partition " +
                "AND ${ENTITY_SET_ID.name} = '$entitySetId' " +
                "AND $LAST_MIGRATE > '$LAST_VALID_MIGRATE' " +
                "LIMIT $BATCH_SIZE"
    }

    private fun updateIdsLastMigrateSql(entitySetId: UUID, partition: Int, ids: Set<UUID>): String {
        return "UPDATE ${IDS.name} SET $LAST_MIGRATE = now() " +
                "WHERE ${PARTITION.name} = $partition " +
                "AND ${ENTITY_SET_ID.name} = '$entitySetId' " +
                "AND ${ID.name} = ANY('{${ids.joinToString(",")}}') "
    }

    private fun colsMatch(col: PostgresColumnDefinition): String {
        return "${DATA.name}.${col.name} = ${IDS.name}.${col.name} "
    }

    private fun deleteSql(immutableEntitySets: Collection<EntitySet>): String {

        val entitySetIds = immutableEntitySets.joinToString(",") { it.id.toString() }

        return "DELETE FROM ${DATA.name} " +
                "USING ${IDS.name} " +
                "WHERE ${IDS.name}.$LAST_MIGRATE > $LAST_VALID_MIGRATE " +
                "AND ${colsMatch(ID)} " +
                "AND ${colsMatch(ENTITY_SET_ID)} " +
                "AND ${colsMatch(PARTITION)} " +
                "AND ${IDS.name}.${ENTITY_SET_ID.name} = ANY( '{$entitySetIds}' ) "
    }

}