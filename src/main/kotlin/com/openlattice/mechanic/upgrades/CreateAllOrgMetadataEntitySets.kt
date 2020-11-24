package com.openlattice.mechanic.upgrades

import com.hazelcast.query.Predicates
import com.openlattice.authorization.*
import com.openlattice.authorization.mapstores.PrincipalMapstore
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.*
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.DataTables
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*

class CreateAllOrgMetadataEntitySets(
        private val toolbox: Toolbox,
        private val organizationService: HazelcastOrganizationService,
        private val entitySetsManager: EntitySetManager,
        private val principalsManager: SecurePrincipalsManager,
        private val authorizationManager: AuthorizationManager,
        private val edmService: EdmManager
) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(CreateAllOrgMetadataEntitySets::class.java)
    }

    private lateinit var organizationMetadataEntityTypeId: UUID
    private lateinit var datasetEntityTypeId: UUID
    private lateinit var columnsEntityTypeId: UUID
    private lateinit var omAuthorizedPropertyTypes: Map<UUID, PropertyType>
    private lateinit var datasetsAuthorizedPropertTypes: Map<UUID, PropertyType>
    private lateinit var columnAuthorizedPropertTypes: Map<UUID, PropertyType>
    private lateinit var propertyTypes: Map<String, PropertyType>

    private val ORGANIZATION_METADATA_ET = FullQualifiedName("ol.organization_metadata")
    private val DATASETS_ET = FullQualifiedName("ol.dataset")
    private val COLUMNS_ET = FullQualifiedName("ol.column")

    override fun upgrade(): Boolean {
        val orgs = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast).toMap()

        val adminRoles = getAdminRolesByOrgId(orgs)

        orgs.values.forEach {
            logger.info("About to initialize metadata entity sets for organization ${it.title} [${it.id}]")
            initializeOrganizationMetadataEntitySets(adminRoles.getValue(it.id))
            logger.info("Finished initializing metadata entity sets for organization ${it.title} [${it.id}]")
        }

        return true
    }

    private fun getAdminRolesByOrgId(organizations: Map<UUID, Organization>): Map<UUID, Role> {
        val rolesByAclKey = HazelcastMap.PRINCIPALS.getMap(toolbox.hazelcast).values(
                Predicates.equal(PrincipalMapstore.PRINCIPAL_TYPE_INDEX, PrincipalType.ROLE)
        ).associateBy { it.aclKey }

        val orgPrincipalNames = organizations.values.map {
            "${it.securablePrincipal.id}|${it.securablePrincipal.name} - ADMIN"
        }.toSet()

        val adminRoles = mutableMapOf<UUID, Role>()
        rolesByAclKey.values.forEach {
            if (orgPrincipalNames.contains(it.principal.id)) {
                adminRoles[it.aclKey[0]] = it as Role
            }
        }

        return organizations.values.associate {
            val aclKey = adminRoles[it.id] ?: createAdminRoleForOrganization(it)
            it.id to aclKey
        }
    }

    private fun createAdminRoleForOrganization(organization: Organization): Role {
        //Create the admin role for the organization and give it ownership of organization.
        val adminRoleAclKey = AclKey(organization.id, UUID.randomUUID())
        val adminRole = createOrganizationAdminRole(organization.securablePrincipal, adminRoleAclKey)

        val userOwnerPrincipals = authorizationManager.getOwnersForSecurableObjects(listOf(organization.getAclKey()))
                .get(organization.getAclKey())
                .filter { it.type == PrincipalType.USER }

        val userOwnerAclKeys = userOwnerPrincipals.map { principalsManager.lookup(it) }.toSet()

        principalsManager.createSecurablePrincipalIfNotExists(userOwnerPrincipals.first(), adminRole)
        principalsManager.addPrincipalToPrincipals(adminRoleAclKey, userOwnerAclKeys)

        authorizationManager.addPermission(adminRoleAclKey, organization.principal, EnumSet.of(Permission.READ))
        authorizationManager.addPermission(organization.getAclKey(), adminRole.principal, EnumSet.allOf(Permission::class.java))

        return adminRole
    }

    private fun createOrganizationAdminRole(organization: SecurablePrincipal, adminRoleAclKey: AclKey): Role {
        val roleId = adminRoleAclKey[1]
        val principalTitle = constructOrganizationAdminRolePrincipalTitle(organization)
        val principalId = constructOrganizationAdminRolePrincipalId(organization)
        val rolePrincipal = Principal(PrincipalType.ROLE, principalId)
        return Role(
                Optional.of(roleId),
                organization.id,
                rolePrincipal,
                principalTitle,
                Optional.of("Administrators of this organization")
        )
    }

    private fun constructOrganizationAdminRolePrincipalTitle(organization: SecurablePrincipal): String {
        return organization.name + " - ADMIN"
    }

    private fun constructOrganizationAdminRolePrincipalId(organization: SecurablePrincipal): String {
        return organization.id.toString() + "|" + constructOrganizationAdminRolePrincipalTitle(organization)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_10_14.value
    }

    private fun initializeFields() {
        if (!this::organizationMetadataEntityTypeId.isInitialized) {
            val om = edmService.getEntityType(ORGANIZATION_METADATA_ET)
            organizationMetadataEntityTypeId = om.id
            omAuthorizedPropertyTypes = edmService.getPropertyTypesAsMap(om.properties)

        }
        if (!this::datasetEntityTypeId.isInitialized) {
            val ds = edmService.getEntityType(DATASETS_ET)
            datasetEntityTypeId = ds.id
            datasetsAuthorizedPropertTypes = edmService.getPropertyTypesAsMap(ds.properties)
        }
        if (!this::columnsEntityTypeId.isInitialized) {
            val c = edmService.getEntityType(COLUMNS_ET)
            columnsEntityTypeId = c.id
            columnAuthorizedPropertTypes = edmService.getPropertyTypesAsMap(c.properties)
        }
        if (!this::propertyTypes.isInitialized) {
            propertyTypes = (omAuthorizedPropertyTypes.values + datasetsAuthorizedPropertTypes.values + columnAuthorizedPropertTypes.values)
                    .associateBy { it.type.fullQualifiedNameAsString }
        }
    }

    fun isFullyInitialized(): Boolean = this::organizationMetadataEntityTypeId.isInitialized &&
            this::datasetEntityTypeId.isInitialized && this::columnsEntityTypeId.isInitialized &&
            this::omAuthorizedPropertyTypes.isInitialized && this::datasetsAuthorizedPropertTypes.isInitialized &&
            this::columnAuthorizedPropertTypes.isInitialized && this::propertyTypes.isInitialized

    fun initializeOrganizationMetadataEntitySets(adminRole: Role) {
        initializeFields()
        if (!isFullyInitialized()) {
            return
        }

        val organizationId = adminRole.organizationId

        val organizationMetadataEntitySetIds = organizationService.getOrganizationMetadataEntitySetIds(organizationId)
        val organizationPrincipal = organizationService.getOrganizationPrincipal(organizationId)!!
        var createdEntitySets = mutableSetOf<UUID>()

        val organizationMetadataEntitySetId = if (organizationMetadataEntitySetIds.organization == UNINITIALIZED_METADATA_ENTITY_SET_ID) {
            val organizationMetadataEntitySet = buildOrganizationMetadataEntitySet(organizationId)
            val id = entitySetsManager.createEntitySet(adminRole.principal, organizationMetadataEntitySet)
            createdEntitySets.add(id)
            id
        } else {
            organizationMetadataEntitySetIds.organization
        }

        val datasetsEntitySetId = if (organizationMetadataEntitySetIds.datasets == UNINITIALIZED_METADATA_ENTITY_SET_ID) {
            val datasetsEntitySet = buildDatasetsEntitySet(organizationId)
            val id = entitySetsManager.createEntitySet(adminRole.principal, datasetsEntitySet)
            createdEntitySets.add(id)
            id
        } else {
            organizationMetadataEntitySetIds.datasets
        }

        val columnsEntitySetId = if (organizationMetadataEntitySetIds.columns == UNINITIALIZED_METADATA_ENTITY_SET_ID) {
            val columnsEntitySet = buildColumnEntitySet(organizationId)
            val id = entitySetsManager.createEntitySet(adminRole.principal, columnsEntitySet)
            createdEntitySets.add(id)
            id
        } else {
            organizationMetadataEntitySetIds.columns
        }

        if (createdEntitySets.isNotEmpty()) {
            organizationService.setOrganizationMetadataEntitySetIds(
                    organizationId,
                    OrganizationMetadataEntitySetIds(
                            organizationMetadataEntitySetId,
                            datasetsEntitySetId,
                            columnsEntitySetId
                    )
            )

            entitySetsManager.getEntitySetsAsMap(createdEntitySets).values.forEach {
                entitySetsManager.setupOrganizationMetadataAndAuditEntitySets(it)
            }
        }
    }

    private fun buildOrganizationMetadataEntitySet(organizationId: UUID): EntitySet = EntitySet(
            organizationId = organizationId,
            entityTypeId = organizationMetadataEntityTypeId,
            name = buildOrganizationMetadataEntitySetName(organizationId),
            _title = "Organization Metadata for $organizationId",
            _description = "Organization Metadata for $organizationId",
            contacts = mutableSetOf(),
            flags = EnumSet.of(EntitySetFlag.METADATA)
    )

    private fun buildDatasetsEntitySet(organizationId: UUID): EntitySet = EntitySet(
            organizationId = organizationId,
            entityTypeId = datasetEntityTypeId,
            name = buildDatasetsEntitySetName(organizationId),
            _title = "Datasets for $organizationId",
            _description = "Datasets for $organizationId",
            contacts = mutableSetOf(),
            flags = EnumSet.of(EntitySetFlag.METADATA)
    )

    private fun buildColumnEntitySet(organizationId: UUID): EntitySet = EntitySet(
            organizationId = organizationId,
            entityTypeId = columnsEntityTypeId,
            name = buildColumnEntitySetName(organizationId),
            _title = "Datasets for $organizationId",
            _description = "Datasets for $organizationId",
            contacts = mutableSetOf(),
            flags = EnumSet.of(EntitySetFlag.METADATA)
    )

    private fun buildOrganizationMetadataEntitySetName(organizationId: UUID): String = DataTables.quote(
            "org-metadata-$organizationId"
    )

    private fun buildDatasetsEntitySetName(organizationId: UUID): String = DataTables.quote("datasets-$organizationId")
    private fun buildColumnEntitySetName(organizationId: UUID): String = DataTables.quote("column-$organizationId")


}