from __future__ import annotations

from enum import Enum, auto
from types import TracebackType
from typing import TYPE_CHECKING, Protocol
from urllib.parse import urlparse

from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport, TSocket

from .internal.ThriftHiveMetastore import Client
from .internal.ttypes import (
    Catalog, Type,
    CreateCatalogRequest,
    AlterCatalogRequest,
    GetCatalogRequest,
    GetCatalogResponse,
    GetCatalogsResponse,
    DropCatalogRequest,
    Database,
    FieldSchema,
    ISchemaName,
    SchemaVersionDescriptor,
    SchemaVersion,
    Table,
    SQLPrimaryKey,
    SQLForeignKey,
    SQLUniqueConstraint,
    SQLNotNullConstraint,
    SQLDefaultConstraint,
    SQLCheckConstraint, AddPrimaryKeyRequest, AddForeignKeyRequest, AddUniqueConstraintRequest,
    AddNotNullConstraintRequest, AddDefaultConstraintRequest, AddCheckConstraintRequest, DropConstraintRequest,
    EnvironmentContext, NoSuchObjectException, GetTableRequest, GetTablesRequest, GetTablesResult, PrimaryKeysRequest,
    PrimaryKeysResponse, ForeignKeysRequest, ForeignKeysResponse, UniqueConstraintsRequest, UniqueConstraintsResponse,
    NotNullConstraintsRequest, NotNullConstraintsResponse, DefaultConstraintsRequest, DefaultConstraintsResponse,
    CheckConstraintsResponse, CheckConstraintsRequest,
)

if TYPE_CHECKING:
    from typing import Self

DEFAULT_CATALOG_NAME = "hive"
DEFAULT_CATALOG_COMMENT = "Default catalog, for Hive"

DEFAULT_DATABASE_NAME = "default"
DEFAULT_DATABASE_COMMENT = "Default Hive database"

DEFAULT_SERIALIZATION_FORMAT = "1"
DATABASE_WAREHOUSE_SUFFIX = ".db"
CAT_DB_TABLE_SEPARATOR = "."
CATALOG_DB_THRIFT_NAME_MARKER = '@'
CATALOG_DB_SEPARATOR = "#"
DB_EMPTY_MARKER = "!"

if TYPE_CHECKING:
    class SupportsCatalog(Protocol):
        catName: str


class TableType(Enum):
    MANAGED_TABLE = auto()
    EXTERNAL_TABLE = auto()
    VIRTUAL_VIEW = auto()
    MATERIALIZED_VIEW = auto()


class ThriftHiveMetastore:
    _transport: TTransport
    _client: Client

    def __init__(self, uri: str):
        url_parts = urlparse(uri)
        transport = TSocket.TSocket(url_parts.hostname, url_parts.port)
        self._transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        self._client = Client(protocol)

    def __enter__(self) -> Self:
        self._transport.open()
        return self

    def __exit__(
        self,
        exctype: type[BaseException] | None,
        excinst: BaseException | None,
        exctb: TracebackType | None
    ) -> None:
        self._transport.close()

    def set_meta_conf(self, key: str, value: str) -> None:
        self._client.setMetaConf(key, value)

    def get_meta_conf(self, key: str) -> str:
        return self._client.getMetaConf(key)

    @staticmethod
    def default_catalog() -> str:
        return DEFAULT_CATALOG_NAME

    @staticmethod
    def _set_catalog(catalog_name: str, o: SupportsCatalog):
        if o.catName is None:
            o.catName = catalog_name

    def prepend_catalog_to_database(
        self,
        database_name: str | None = None,
        catalog_name: str | None = None,
    ) -> str:
        if catalog_name is None:
            catalog_name = self.default_catalog()
        buf = f"{CATALOG_DB_THRIFT_NAME_MARKER}{catalog_name}{CATALOG_DB_SEPARATOR}"
        if database_name is not None:
            buf += database_name if len(database_name) > 0 else DB_EMPTY_MARKER
        return buf

    # region catalog
    def create_catalog(self, catalog: Catalog) -> None:
        self._client.create_catalog(CreateCatalogRequest(catalog))

    def alter_catalog(self, catalog_name: str, new_catalog: Catalog) -> None:
        self._client.alter_catalog(AlterCatalogRequest(catalog_name, new_catalog))

    def get_catalog(self, catalog_name: str) -> Catalog | None:
        res: GetCatalogResponse = self._client.get_catalog(GetCatalogRequest(catalog_name))
        if res is None:
            return None
        return res.catalog  # TODO: implement filterHook.filterCatalog

    def list_catalogs(self) -> list[str] | None:
        res: GetCatalogsResponse = self._client.get_catalogs()
        if res is None:
            return None
        return res.names  # TODO: implement filterHook.filterCatalogs

    def drop_catalog(self, catalog_name: str) -> None:
        self._client.drop_catalog(DropCatalogRequest(catalog_name))

    # endregion catalog
    # region database
    def create_database(self, database: Database) -> None:
        if database.catalogName is None:
            database.catalogName = self.default_catalog()
        self._client.create_database(database)

    def alter_database(self, database_name: str, new_database: Database, catalog_name: str | None = None):
        database_fqdn = self.prepend_catalog_to_database(database_name, catalog_name)
        self._client.alter_database(database_fqdn, new_database)

    def get_database(self, database_name: str, catalog_name: str | None = None) -> Database | None:
        database_fqdn = self.prepend_catalog_to_database(database_name, catalog_name)
        return self._client.get_database(database_fqdn)  # TODO: implement filterHook.filterDatabase

    def list_databases(self, catalog_name: str | None = None, pattern: str | None = None) -> list[str]:
        database_pattern = self.prepend_catalog_to_database(pattern, catalog_name)
        return self._client.get_databases(database_pattern)  # TODO: implement filterHook.filterDatabases

    def drop_database(
        self,
        database_name: str, catalog_name: str | None = None,
        *,
        delete_data: bool = True,
        ignore_unknown_database: bool = False,
        cascade: bool = False,
    ) -> None:
        try:
            self.get_database(database_name, catalog_name)
        except NoSuchObjectException as e:
            if ignore_unknown_database:
                return
            raise e

        # TODO: implement cascade
        database_fqdn = self.prepend_catalog_to_database(database_name, catalog_name)
        self._client.drop_database(database_fqdn, delete_data, cascade)

    # endregion database
    # region table
    def create_table(
        self,
        table: Table,
        *,
        primary_keys: list[SQLPrimaryKey] | None = None,
        foreign_keys: list[SQLForeignKey] | None = None,
        unique_constraints: list[SQLUniqueConstraint] | None = None,
        not_null_constraints: list[SQLNotNullConstraint] | None = None,
        default_constraints: list[SQLDefaultConstraint] | None = None,
        check_constraints: list[SQLCheckConstraint] | None = None,
    ) -> None:
        if table.catName is None:
            default_catalog = self.default_catalog()
            table.catName = default_catalog

            if primary_keys is not None:
                map(lambda c: self._set_catalog(default_catalog, c), primary_keys)

            if foreign_keys is not None:
                map(lambda c: self._set_catalog(default_catalog, c), foreign_keys)

            if unique_constraints is not None:
                map(lambda c: self._set_catalog(default_catalog, c), unique_constraints)

            if not_null_constraints is not None:
                map(lambda c: self._set_catalog(default_catalog, c), not_null_constraints)

            if default_constraints is not None:
                map(lambda c: self._set_catalog(default_catalog, c), default_constraints)

            if check_constraints is not None:
                map(lambda c: self._set_catalog(default_catalog, c), check_constraints)

        self._client.create_table_with_constraints(
            table,
            primary_keys, foreign_keys,
            unique_constraints, not_null_constraints,
            default_constraints, check_constraints,
        )

    def alter_table(
        self,
        table_name: str, new_table: Table, database_name: str, catalog_name: str | None = None,
        *,
        cascade: bool = False,
        env_context: EnvironmentContext | None = None,
    ) -> None:
        if cascade:
            if env_context is None:
                env_context = EnvironmentContext()
            env_context.properties["CASCADE"] = True
        database_fqdn = self.prepend_catalog_to_database(database_name, catalog_name)
        self._client.alter_table_with_environment_context(database_fqdn, table_name, new_table, env_context)

    def drop_table(
        self,
        table_name: str, database_name: str, catalog_name: str | None = None,
        *,
        delete_data: bool = True,
        ignore_unknown_table: bool = False,
        if_purge: bool = False,
    ) -> None:
        try:
            self.get_table(table_name, database_name, catalog_name)
        except NoSuchObjectException as e:
            if ignore_unknown_table:
                return
            raise e

        # Table exist
        database_fqdn = self.prepend_catalog_to_database(database_name, catalog_name)
        env_context = EnvironmentContext({"ifPurge": "TRUE"}) if if_purge else None
        self._client.drop_table_with_environment_context(table_name, database_fqdn, delete_data, env_context)

    def truncate_table(self, partition_names: list[str],
                       table_name: str, database_name: str, catalog_name: str | None = None) -> None:
        database_fqdn = self.prepend_catalog_to_database(database_name, catalog_name)
        self._client.truncate_table(database_fqdn, table_name, partition_names)

    def get_table(self, table_name: str, database_name: str, catalog_name: str | None = None) -> Table:
        database_fqdn = self.prepend_catalog_to_database(database_name, catalog_name)
        return self._client.get_table(database_fqdn, table_name)

    def get_tables(self, table_names: list[str], database_name: str, catalog_name: str | None = None) -> list[Table]:
        res: GetTablesResult = self._client.get_table_objects_by_name_req(
            GetTablesRequest(database_name, table_names, None, catalog_name)
        )
        return res.tables  # TODO: implement filterHook.filterTables

    def list_tables(
        self,
        database_name: str, catalog_name: str | None = None,
        *,
        table_pattern: str | None = None,
        table_type: TableType | None = None
    ) -> list[str]:
        database_fqdn = self.prepend_catalog_to_database(database_name, catalog_name)
        if table_type is not None:
            return self._client.get_tables_by_type(database_fqdn, table_pattern, str(table_type))
        else:
            return self._client.get_tables(database_fqdn, table_pattern)

    # endregion table
    # region constraints
    def add_primary_key(self, primary_keys: list[SQLPrimaryKey]) -> None:
        default_catalog = self.default_catalog()
        map(lambda c: self._set_catalog(default_catalog, c), primary_keys)
        self._client.add_primary_key(AddPrimaryKeyRequest(primary_keys))

    def get_primary_keys(self, table_name: str, database_name: str,
                         catalog_name: str | None = None) -> list[SQLPrimaryKey]:
        req = PrimaryKeysRequest(
            database_name,
            table_name,
            catalog_name
        )
        res: PrimaryKeysResponse = self._client.get_primary_keys(req)
        return res.primaryKeys

    def add_foreign_key(self, foreign_keys: list[SQLForeignKey]) -> None:
        default_catalog = self.default_catalog()
        map(lambda c: self._set_catalog(default_catalog, c), foreign_keys)
        self._client.add_foreign_key(AddForeignKeyRequest(foreign_keys))

    def get_foreign_keys(
        self,
        parent_table_name: str, parent_database_name: str,
        foreign_table_name: str, foreign_database_name: str,
        catalog_name: str | None = None
    ) -> list[SQLForeignKey]:
        req = ForeignKeysRequest(
            parent_database_name, parent_table_name,
            foreign_database_name, foreign_table_name,
            catalog_name
        )
        res: ForeignKeysResponse = self._client.get_foreign_keys(req)
        return res.foreignKeys

    def add_unique_constraint(self, unique_constraints: list[SQLUniqueConstraint]) -> None:
        default_catalog = self.default_catalog()
        map(lambda c: self._set_catalog(default_catalog, c), unique_constraints)
        self._client.add_unique_constraint(AddUniqueConstraintRequest(unique_constraints))

    def get_unique_constraints(self, table_name: str, database_name: str,
                               catalog_name: str | None = None) -> list[SQLUniqueConstraint]:
        req = UniqueConstraintsRequest(
            catalog_name or self.default_catalog(),
            database_name,
            table_name,
        )
        res: UniqueConstraintsResponse = self._client.get_unique_constraints(req)
        return res.uniqueConstraints

    def add_not_null_constraint(self, not_null_constraints: list[SQLNotNullConstraint]) -> None:
        default_catalog = self.default_catalog()
        map(lambda c: self._set_catalog(default_catalog, c), not_null_constraints)
        self._client.add_not_null_constraint(AddNotNullConstraintRequest(not_null_constraints))

    def get_not_null_constraints(self, table_name: str, database_name: str,
                                 catalog_name: str | None = None) -> list[SQLNotNullConstraint]:
        req = NotNullConstraintsRequest(
            catalog_name or self.default_catalog(),
            database_name,
            table_name,
        )
        res: NotNullConstraintsResponse = self._client.get_not_null_constraints(req)
        return res.notNullConstraints

    def add_default_constraint(self, default_constraints: list[SQLDefaultConstraint]) -> None:
        default_catalog = self.default_catalog()
        map(lambda c: self._set_catalog(default_catalog, c), default_constraints)
        self._client.add_default_constraint(AddDefaultConstraintRequest(default_constraints))

    def get_default_constraints(self, table_name: str, database_name: str,
                                catalog_name: str | None = None) -> list[SQLDefaultConstraint]:
        req = DefaultConstraintsRequest(
            catalog_name or self.default_catalog(),
            database_name,
            table_name,
        )
        res: DefaultConstraintsResponse = self._client.get_default_constraints(req)
        return res.defaultConstraints

    def add_check_constraint(self, check_constraints: list[SQLCheckConstraint]) -> None:
        default_catalog = self.default_catalog()
        map(lambda c: self._set_catalog(default_catalog, c), check_constraints)
        self._client.add_check_constraint(AddCheckConstraintRequest(check_constraints))

    def get_check_constraints(self, table_name: str, database_name: str,
                              catalog_name: str | None = None) -> list[SQLCheckConstraint]:
        req = CheckConstraintsRequest(
            catalog_name or self.default_catalog(),
            database_name,
            table_name,
        )
        res: CheckConstraintsResponse = self._client.get_check_constraints(req)
        return res.checkConstraints

    def drop_constraint(self, constraint_name: str,
                        table_name: str, database_name: str, catalog_name: str | None = None):
        catalog_name = catalog_name or self.default_catalog()
        self._client.drop_constraint(DropConstraintRequest(database_name, table_name, constraint_name, catalog_name))

    # endregion constraints

    def create_type(self, type: Type) -> bool:
        return self._client.create_type(type)

    def get_types(self, type_name: str) -> dict[str, Type]:
        return self._client.get_type_all(type_name)

    def get_type(self, type_name: str) -> Type:
        return self._client.get_type(type_name)

    def drop_type(self, type_name: str) -> bool:
        return self._client.drop_type(type_name)

    def get_fields(self, table_name: str, database_name: str, catalog_name: str | None = None) -> list[FieldSchema]:
        database_fqdn = self.prepend_catalog_to_database(database_name, catalog_name)
        return self._client.get_fields(database_fqdn, table_name)

    def get_schema(self, table_name: str, database_name: str, catalog_name: str | None = None) -> list[FieldSchema]:
        database_fqdn = self.prepend_catalog_to_database(database_name, catalog_name)
        return self._client.get_schema(database_fqdn, table_name)

    def add_schema_version(self, schema_version: SchemaVersion) -> None:
        self._set_catalog(self.default_catalog(), schema_version.schema)
        self._client.add_schema_version(schema_version)

    def get_schema_version(self, version: int, schema_name: str,
                           database_name: str, catalog_name: str | None = None) -> SchemaVersion:
        return self._client.get_schema_version(
            SchemaVersionDescriptor(
                ISchemaName(catalog_name, database_name, schema_name),
                version
            )
        )

    def get_schema_version_latest(self, schema_name: str,
                                  database_name: str, catalog_name: str | None = None) -> SchemaVersion:
        return self._client.get_schema_latest_version(
            SchemaVersionDescriptor(
                ISchemaName(catalog_name, database_name, schema_name),
            )
        )

    def get_schema_versions(self, schema_name: str,
                            database_name: str, catalog_name: str | None = None) -> list[SchemaVersion]:
        return self._client.get_schema_all_versions(
            SchemaVersionDescriptor(
                ISchemaName(catalog_name, database_name, schema_name),
            )
        )

    def drop_schema_version(self, version: int, schema_name: str,
                            database_name: str, catalog_name: str | None = None) -> None:
        self._client.drop_schema_version(
            SchemaVersionDescriptor(
                ISchemaName(catalog_name, database_name, schema_name),
                version
            )
        )

    # def get_schemas_by_cols
