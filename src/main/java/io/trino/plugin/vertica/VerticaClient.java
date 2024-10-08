/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.vertica;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.math.LongMath;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.BooleanReadFunction;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DoubleReadFunction;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.ReadFunction;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.SliceReadFunction;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCorr;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementCovariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementCovarianceSamp;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementRegrIntercept;
import io.trino.plugin.jdbc.aggregation.ImplementRegrSlope;
import io.trino.plugin.jdbc.aggregation.ImplementStddevPop;
import io.trino.plugin.jdbc.aggregation.ImplementStddevSamp;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.aggregation.ImplementVariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementVarianceSamp;
import io.trino.plugin.jdbc.expression.ComparisonOperator;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.expression.RewriteComparison;
import io.trino.plugin.jdbc.expression.RewriteIn;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingSqlDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunctionUsingSqlTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.toTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.vertica.VerticaSessionProperties.isEnableConvertDecimalToVarchar;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;

public class VerticaClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(VerticaClient.class);
    private static final int ARRAY_RESULT_SET_VALUE_COLUMN = 2;
    private static final int POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION = 6;
    private static final int TRINO_MAX_DECIMAL_PRECISION = 38;
    private final Type uuidType;
    private final List<String> tableTypes;
    private final boolean statisticsEnabled;
    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;

    @Inject
    public VerticaClient(
            BaseJdbcConfig config,
            //VerticaConfig verticaConfig,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier remoteQueryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, remoteQueryModifier, true);
        ImmutableList.Builder<String> tableTypes = ImmutableList.builder();
        tableTypes.add("TABLE", "VIEW", "SYSTEM TABLE");
        this.tableTypes = tableTypes.build();
        this.statisticsEnabled = statisticsConfig.isEnabled();
        log.info("statisticsEnabled:" + this.statisticsEnabled);
        this.uuidType = typeManager.getType(new TypeSignature(StandardTypes.UUID));

//        Predicate<ConnectorSession> convertDecimalToVarcharEnabled = VerticaSessionProperties::isEnableConvertDecimalToVarchar;
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                // TODO allow all comparison operators for numeric types
                // TODO add timestamp comparison with string
                .add(new RewriteComparison(ImmutableSet.of(ComparisonOperator.EQUAL, ComparisonOperator.NOT_EQUAL)))
                .add(new RewriteIn())
                .withTypeClass("integer_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint"))
                .withTypeClass("numeric_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint", "decimal", "double"))
                .map("$add(left: integer_type, right: integer_type)").to("left + right")
                .map("$subtract(left: integer_type, right: integer_type)").to("left - right")
                .map("$multiply(left: integer_type, right: integer_type)").to("left * right")
                .map("$divide(left: integer_type, right: integer_type)").to("left / right")
                .map("$modulus(left: integer_type, right: integer_type)").to("left % right")
                .map("$negate(value: integer_type)").to("-value")
                .map("$like(value: varchar, pattern: varchar): boolean").to("value LIKE pattern")
                .map("$like(value: varchar, pattern: varchar, escape: varchar(1)): boolean").to("value LIKE pattern ESCAPE escape")
                .map("$not($is_null(value))").to("value IS NOT NULL")
                .map("$not(value: boolean)").to("NOT value")
                .map("$is_null(value)").to("value IS NULL")
                .map("$nullif(first, second)").to("NULLIF(first, second)")
                .map("$less_than(left: numeric_type, right: numeric_type)").to("left < right")
                .map("$equal(left: date, right: varchar)").to("left = right")
                .map("$less_than(left: date, right: varchar)").to("left < right")
                .map("$more_than(left: date, right: varchar)").to("left > right")
                .build();

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, false))
                        .add(new ImplementSum(VerticaClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())
                        .add(new ImplementCovarianceSamp())
                        .add(new ImplementCovariancePop())
                        .add(new ImplementCorr())
                        .add(new ImplementRegrIntercept())
                        .add(new ImplementRegrSlope())
                        .build());
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));
        log.info("typeHandle(1):" + typeHandle.toString());
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        switch (jdbcTypeName) {
            case "uuid":
            case "Uuid":
                return Optional.of(uuidColumnMapping());
        }
        switch (typeHandle.jdbcType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC:
            case Types.DECIMAL:
            {
                int columnSize = typeHandle.requiredColumnSize();
                int precision;
                int decimalDigits = typeHandle.decimalDigits().orElse(0);
                precision = columnSize + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                log.info("decimal type (" + precision + "," + max(decimalDigits, 0) + ")");
                if (precision > TRINO_MAX_DECIMAL_PRECISION || precision <= 0) {
                    if (isEnableConvertDecimalToVarchar(session) == true) {
                        return mapToUnboundedVarchar(typeHandle);
                    }
                    return Optional.of(doubleColumnMapping());
                }

                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0)), UNNECESSARY));
            }

            case Types.CHAR:
                return Optional.of(charColumnMapping(typeHandle.requiredColumnSize()));

            case Types.LONGVARCHAR:
            case Types.VARCHAR:
                return Optional.of(varcharColumnMapping(typeHandle.requiredColumnSize()));

            case Types.LONGVARBINARY:
            case Types.BINARY:
            case Types.VARBINARY:
                return Optional.of(varbinaryColumnMapping());

            case Types.DATE:
                return Optional.of(dateColumnMappingUsingSqlDate());

            case Types.TIME:
                return Optional.of(timeColumnMapping(typeHandle.requiredDecimalDigits()));

            case Types.TIMESTAMP:
                return Optional.of(timestampColumnMappingUsingSqlTimestampWithRounding(TIMESTAMP_MICROS));

            case Types.ARRAY:
                return Optional.empty();
                /*Optional<ColumnMapping> columnMapping = arrayToTrinoType(session, connection, typeHandle);
                if (columnMapping.isPresent()) {
                    return columnMapping;
                }
                break;*/
        }

        log.debug("typeHandle(2):notSupported:" + typeHandle.toString());
        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    private Optional<ColumnMapping> arrayToTrinoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        checkArgument(typeHandle.jdbcType() == Types.ARRAY, "Not array type");
        log.debug("session = " + session.toString());
        // resolve and map base array element type
        JdbcTypeHandle baseElementTypeHandle = getArrayElementTypeHandle(connection, typeHandle);
        String baseElementTypeName = baseElementTypeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Element type name is missing: " + baseElementTypeHandle));
        Optional<ColumnMapping> baseElementMapping = toColumnMapping(session, connection, baseElementTypeHandle);
        // Vertica doesn't return the array dimensions this way... have to assume it's 1 here?
        /*if (typeHandle.getArrayDimensions().isEmpty()) {
            log.debug("empty array dimensions?");
            return Optional.empty();
        }*/
        return baseElementMapping
                .map(elementMapping -> {
                    ArrayType trinoArrayType = new ArrayType(elementMapping.getType());
                    ColumnMapping arrayColumnMapping = arrayColumnMapping(session, trinoArrayType, elementMapping, baseElementTypeName);

                    int arrayDimensions = typeHandle.arrayDimensions().get();
                    for (int i = 1; i < arrayDimensions; i++) {
                        trinoArrayType = new ArrayType(trinoArrayType);
                        arrayColumnMapping = arrayColumnMapping(session, trinoArrayType, arrayColumnMapping, baseElementTypeName);
                    }
                    return arrayColumnMapping;
                });
    }

    private static JdbcTypeHandle getArrayElementTypeHandle(Connection connection, JdbcTypeHandle arrayTypeHandle)
    {
        String jdbcTypeName = arrayTypeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + arrayTypeHandle));
        log.debug("### JDBC array type handle = " + arrayTypeHandle.toString());
        return new JdbcTypeHandle(
                Types.INTEGER,
                Optional.of("Integer"),
                arrayTypeHandle.columnSize(),
                arrayTypeHandle.decimalDigits(),
                Optional.of(1),
                Optional.empty());
        /*
        try {
            TypeInfo typeInfo = connection.unwrap(PgConnection.class).getTypeInfo();
            int pgElementOid = typeInfo.getPGArrayElement(typeInfo.getPGType(jdbcTypeName));
            verify(arrayTypeHandle.getCaseSensitivity().isEmpty(), "Case sensitivity not supported");
            return new JdbcTypeHandle(
                    typeInfo.getSQLType(pgElementOid),
                    Optional.of(typeInfo.getPGType(pgElementOid)),
                    arrayTypeHandle.ColumnSize(),
                    arrayTypeHandle.DecimalDigits(),
                    arrayTypeHandle.ArrayDimensions(),
                    Optional.empty());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
        */
    }

    private static ColumnMapping arrayColumnMapping(ConnectorSession session, ArrayType arrayType, ColumnMapping arrayElementMapping, String baseElementJdbcTypeName)
    {
        return ColumnMapping.objectMapping(
                arrayType,
                arrayReadFunction(arrayType.getElementType(), arrayElementMapping.getReadFunction()),
                arrayWriteFunction(session, arrayType.getElementType(), baseElementJdbcTypeName));
    }

    private static ObjectReadFunction arrayReadFunction(Type elementType, ReadFunction elementReadFunction)
    {
        return ObjectReadFunction.of(Block.class, (resultSet, columnIndex) -> {
            Array array = resultSet.getArray(columnIndex);
            BlockBuilder builder = elementType.createBlockBuilder(null, 10);
            try (ResultSet arrayAsResultSet = array.getResultSet()) {
                while (arrayAsResultSet.next()) {
                    if (elementReadFunction.isNull(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN)) {
                        builder.appendNull();
                    }
                    else if (elementType.getJavaType() == boolean.class) {
                        elementType.writeBoolean(builder, ((BooleanReadFunction) elementReadFunction).readBoolean(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else if (elementType.getJavaType() == long.class) {
                        elementType.writeLong(builder, ((LongReadFunction) elementReadFunction).readLong(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else if (elementType.getJavaType() == double.class) {
                        elementType.writeDouble(builder, ((DoubleReadFunction) elementReadFunction).readDouble(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else if (elementType.getJavaType() == Slice.class) {
                        elementType.writeSlice(builder, ((SliceReadFunction) elementReadFunction).readSlice(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else {
                        elementType.writeObject(builder, ((ObjectReadFunction) elementReadFunction).readObject(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                }
            }

            return builder.build();
        });
    }

    private static ObjectWriteFunction arrayWriteFunction(ConnectorSession session, Type elementType, String baseElementJdbcTypeName)
    {
        return ObjectWriteFunction.of(Block.class, (statement, index, block) -> {
            Array jdbcArray = null; //statement.getConnection().createArrayOf(baseElementJdbcTypeName, getJdbcObjectArray(session, elementType, block));
            statement.setArray(index, jdbcArray);
        });
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }

        if (type == TINYINT) {
            // PostgreSQL has no type corresponding to tinyint
            return WriteMapping.longMapping("smallint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }

        if (type == REAL) {
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }

        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type instanceof CharType) {
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction());
        }

        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar";
            }
            else {
                dataType = "varchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("bytea", varbinaryWriteFunction());
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
        }

        if (type instanceof TimeType timeType) {
            if (timeType.getPrecision() <= POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION) {
                return WriteMapping.longMapping(format("time(%s)", timeType.getPrecision()), timeWriteFunction(timeType.getPrecision()));
            }
            return WriteMapping.longMapping(format("time(%s)", POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION), timeWriteFunction(POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION));
        }

        if (type instanceof TimestampType timestampType) {
            if (timestampType.getPrecision() <= POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION) {
                verify(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION);
                return WriteMapping.longMapping(format("timestamp(%s)", timestampType.getPrecision()), VerticaClient::shortTimestampWriteFunction);
            }
            verify(timestampType.getPrecision() > TimestampType.MAX_SHORT_PRECISION);
            return WriteMapping.objectMapping(format("timestamp(%s)", POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION), longTimestampWriteFunction());
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            if (timestampWithTimeZoneType.getPrecision() <= POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION) {
                String dataType = format("timestamptz(%d)", timestampWithTimeZoneType.getPrecision());
                if (timestampWithTimeZoneType.getPrecision() <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
                    return WriteMapping.longMapping(dataType, shortTimestampWithTimeZoneWriteFunction());
                }
                return WriteMapping.objectMapping(dataType, longTimestampWithTimeZoneWriteFunction());
            }
            return WriteMapping.objectMapping(format("timestamptz(%d)", POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION), longTimestampWithTimeZoneWriteFunction());
        }
        if (type.equals(uuidType)) {
            return WriteMapping.sliceMapping("uuid", uuidWriteFunction());
        }
        /*if (type instanceof ArrayType arrayType && getArrayMapping(session) == AS_ARRAY) {
            Type elementType = arrayType.getElementType();
            String elementDataType = toWriteMapping(session, elementType).getDataType();
            return WriteMapping.objectMapping(elementDataType + "[]", arrayWriteFunction(session, elementType, getArrayElementPgTypeName(session, this, elementType)));
        }*/

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    private static ColumnMapping charColumnMapping(int charLength)
    {
        if (charLength > CharType.MAX_LENGTH) {
            return varcharColumnMapping(charLength);
        }
        CharType charType = createCharType(charLength);
        return ColumnMapping.sliceMapping(
                charType,
                charReadFunction(charType),
                charWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    private static ColumnMapping varcharColumnMapping(int varcharLength)
    {
        VarcharType varcharType = varcharLength <= VarcharType.MAX_LENGTH
                ? createVarcharType(varcharLength)
                : createUnboundedVarcharType();
        return ColumnMapping.sliceMapping(
                varcharType,
                varcharReadFunction(varcharType),
                varcharWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    private static ColumnMapping timestampColumnMappingUsingSqlTimestampWithRounding(TimestampType timestampType)
    {
        // TODO support higher precision
        checkArgument(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return ColumnMapping.longMapping(
                timestampType,
                (resultSet, columnIndex) -> {
                    LocalDateTime localDateTime = resultSet.getTimestamp(columnIndex).toLocalDateTime();
                    int roundedNanos = toIntExact(round(localDateTime.getNano(), 9 - timestampType.getPrecision()));
                    LocalDateTime rounded = localDateTime
                            .withNano(0)
                            .plusNanos(roundedNanos);
                    return toTrinoTimestamp(timestampType, rounded);
                },
                timestampWriteFunctionUsingSqlTimestamp(timestampType),
                FULL_PUSHDOWN);
    }

    private static ColumnMapping timeColumnMapping(int precision)
    {
        verify(precision <= 6, "Unsupported precision: %s", precision); // PostgreSQL limit but also assumption within this method
        return ColumnMapping.longMapping(
                createTimeType(precision),
                (resultSet, columnIndex) -> {
                    LocalTime time = resultSet.getObject(columnIndex, LocalTime.class);
                    long nanosOfDay = time.toNanoOfDay();
                    if (nanosOfDay == NANOSECONDS_PER_DAY - 1) {
                        // PostgreSQL's 24:00:00 is returned as 23:59:59.999999999, regardless of column precision
                        nanosOfDay = NANOSECONDS_PER_DAY - LongMath.pow(10, 9 - precision);
                    }

                    long picosOfDay = nanosOfDay * PICOSECONDS_PER_NANOSECOND;
                    return round(picosOfDay, 12 - precision);
                },
                timeWriteFunction(precision),
                // Pushdown disabled because PostgreSQL distinguishes TIME '24:00:00' and TIME '00:00:00' whereas Trino does not.
                DISABLE_PUSHDOWN);
    }

    private static void shortTimestampWriteFunction(PreparedStatement statement, int index, long epochMicros)
            throws SQLException
    {
        LocalDateTime localDateTime = fromTrinoTimestamp(epochMicros);
        statement.setTimestamp(index, Timestamp.valueOf(localDateTime.toString()));
    }

    private static ObjectWriteFunction longTimestampWriteFunction()
    {
        return ObjectWriteFunction.of(LongTimestamp.class, ((statement, index, timestamp) -> {
            // PostgreSQL supports up to 6 digits of precision
            //noinspection ConstantConditions
            verify(POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION == 6);

            long epochMicros = timestamp.getEpochMicros();
            if (timestamp.getPicosOfMicro() >= PICOSECONDS_PER_MICROSECOND / 2) {
                epochMicros++;
            }
            shortTimestampWriteFunction(statement, index, epochMicros);
        }));
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            // PostgreSQL does not store zone information in "timestamp with time zone" data type
            long millisUtc = unpackMillisUtc(value);
            statement.setTimestamp(index, new Timestamp(millisUtc));
        };
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction()
    {
        return ObjectWriteFunction.of(
                LongTimestampWithTimeZone.class,
                (statement, index, value) -> {
                    // PostgreSQL does not store zone information in "timestamp with time zone" data type
                    long epochSeconds = floorDiv(value.getEpochMillis(), MILLISECONDS_PER_SECOND);
                    long nanosOfSecond = floorMod(value.getEpochMillis(), MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND + value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
                    statement.setObject(index, OffsetDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanosOfSecond), UTC_KEY.getZoneId()));
                });
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        // Postgres sorts textual types differently compared to Trino so we cannot safely pushdown any aggregations which take a text type as an input or as part of grouping set
        //return preventTextualTypeAggregationPushdown(groupingSets);
        // However, we'll let it pass to Vertica...
        return true;
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream()
                    .map(sortItem -> {
                        String ordering = sortItem.sortOrder().isAscending() ? "ASC" : "DESC";
                        String nullsHandling = "";
                        String collation = "";
                        return format("%s %s %s %s", quoted(sortItem.column().getColumnName()), collation, ordering, nullsHandling);
                    })
                    .collect(joining(", "));
            return format("%s ORDER BY %s LIMIT %d", query, orderBy, limit);
        });
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
    {
        if (!statisticsEnabled) {
            return TableStatistics.empty();
        }
        if (!handle.isNamedRelation()) {
            return TableStatistics.empty();
        }
        try {
            return readTableStatistics(session, handle);
        }
        catch (SQLException | RuntimeException e) {
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(JDBC_ERROR, "Failed fetching statistics for table: " + handle, e);
        }
    }

    private TableStatistics readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            StatisticsDao statisticsDao = new StatisticsDao(handle);

            Optional<Long> optionalRowCount = readRowCountTableStat(statisticsDao, table);
            if (optionalRowCount.isEmpty()) {
                // Table not found
                return TableStatistics.empty();
            }
            long rowCount = optionalRowCount.get();
            if (rowCount == -1) {
                // Table has never yet been vacuumed or analyzed
                return TableStatistics.empty();
            }
            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));

            if (rowCount == 0) {
                return tableStatistics.build();
            }

            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            Map<String, ColumnStatisticsResult> columnStatistics = statisticsDao.getColumnStatistics(remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName()).stream()
                    .collect(toImmutableMap(ColumnStatisticsResult::getColumnName, identity()));

            for (JdbcColumnHandle column : this.getColumns(session, table)) {
                ColumnStatisticsResult result = columnStatistics.get(column.getColumnName());
                if (result == null) {
                    continue;
                }

                ColumnStatistics statistics = ColumnStatistics.builder()
                        .setNullsFraction(result.getNullsFraction()
                                .map(Estimate::of)
                                .orElseGet(Estimate::unknown))
                        .setDistinctValuesCount(result.getDistinctValuesIndicator()
                                .map(distinctValuesIndicator -> {
                                    if (distinctValuesIndicator >= 0.0) {
                                        return distinctValuesIndicator;
                                    }
                                    return -distinctValuesIndicator * rowCount;
                                })
                                .map(Estimate::of)
                                .orElseGet(Estimate::unknown))
                        .setDataSize(result.getAverageColumnLength()
                                .flatMap(averageColumnLength ->
                                        result.getNullsFraction().map(nullsFraction ->
                                                Estimate.of(1.0 * averageColumnLength * rowCount * (1 - nullsFraction))))
                                .orElseGet(Estimate::unknown))
                        .build();

                tableStatistics.setColumnStatistics(column, statistics);
            }

            return tableStatistics.build();
        }
    }

    private static Optional<Long> readRowCountTableStat(StatisticsDao statisticsDao, JdbcTableHandle table)
    {
        RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
        String schemaName = remoteTableName.getSchemaName().orElse(null);
        Optional<Long> rowCount = statisticsDao.getRowCount(schemaName, remoteTableName.getTableName());
        log.info("rowCount for " + schemaName + "." + remoteTableName.getTableName() + ":" + rowCount.toString());
        if (rowCount.isEmpty()) {
            // Table not found
            return Optional.empty();
        }
        return rowCount;
    }

    @Override
    public Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            Map<JdbcColumnHandle, String> leftProjections,
            PreparedQuery rightSource,
            Map<JdbcColumnHandle, String> rightProjections,
            List<ParameterizedExpression> joinConditions,
            JoinStatistics statistics)
    {
        if (joinType == JoinType.FULL_OUTER) {
            // FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
            return Optional.empty();
        }
        return implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.implementJoin(session, joinType, leftSource, leftProjections, rightSource, rightProjections, joinConditions, statistics));
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        boolean isVarchar = Stream.of(joinCondition.getLeftColumn(), joinCondition.getRightColumn())
                .map(JdbcColumnHandle::getColumnType)
                .anyMatch(type -> type instanceof CharType || type instanceof VarcharType);
        if (isVarchar) {
            // PostgreSQL is case sensitive by default, but orders varchars differently
            JoinCondition.Operator operator = joinCondition.getOperator();
            switch (operator) {
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case EQUAL:
                case NOT_EQUAL:
                case IDENTICAL:
                    return true;
            }
            return false;
        }

        return true;
    }

    private static SliceWriteFunction uuidWriteFunction()
    {
        return (statement, index, value) -> statement.setObject(index, trinoUuidToJavaUuid(value), Types.OTHER);
    }

    private ColumnMapping uuidColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                uuidType,
                (resultSet, columnIndex) -> javaUuidToTrinoUuid((UUID) resultSet.getObject(columnIndex)),
                uuidWriteFunction());
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        execute(session, buildTableCommentSql(handle.asPlainTable().getRemoteTableName(), comment));
    }

    private String buildTableCommentSql(RemoteTableName remoteTableName, Optional<String> comment)
    {
        return format(
                "COMMENT ON TABLE %s IS %s",
                quoted(remoteTableName),
                comment.map(BaseJdbcClient::varcharLiteral).orElse("NULL"));
    }

    private static class StatisticsDao
    {
        private final Handle handle;

        public StatisticsDao(Handle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        Optional<Long> getRowCount(String schema, String tableName)
        {
            return handle.createQuery("select ls.\"row_count\"\n" +
                            "        from \"v_internal\".\"vs_logical_statistics\" as ls\n" +
                            "        left join \"v_internal\".\"vs_tables\" as t\n" +
                            "        on ls.\"table_oid\" = t.\"oid\"\n" +
                            "        left join \"v_internal\".\"vs_schemata\" as s\n" +
                            "        on t.\"schema\" = s.\"oid\"\n" +
                            "        where s.\"name\" = :schema and t.\"name\" = :table_name and ls.\"is_row_count_valid\"")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .mapTo(Long.class)
                    .findOne();
        }

        List<ColumnStatisticsResult> getColumnStatistics(String schema, String tableName)
        {
            /*
            select pc.name,pc.min,pc.max,pc.ndv,pc.typelen from vs_projections p join vs_projection_columns pc on p.oid = pc.proj where p.schemaname = :schema and p.anchortablename = :table_name and pc.stat_type = 'FULL'
             */
            return handle.createQuery("select pc.name,pc.min,pc.max,pc.ndv,pc.typelen from vs_projections p join vs_projection_columns pc on p.oid = pc.proj where p.schemaname = :schema and p.anchortablename = :table_name and pc.stat_type = 'FULL'")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .map((rs, ctx) -> new ColumnStatisticsResult(
                            requireNonNull(rs.getString("name"), "name is null"),
                            //Optional.ofNullable(rs.getObject("null_frac", Float.class)),
                            Optional.ofNullable(rs.getObject("ndv", Float.class)),
                            Optional.ofNullable(rs.getObject("typelen", Integer.class))))
                    .list();
        }
    }

    private static class ColumnStatisticsResult
    {
        private final String columnName;
        private final Optional<Float> nullsFraction;
        private final Optional<Float> distinctValuesIndicator;
        private final Optional<Integer> averageColumnLength;

        public ColumnStatisticsResult(String columnName, Optional<Float> nullsFraction, Optional<Float> distinctValuesIndicator, Optional<Integer> averageColumnLength)
        {
            this.columnName = columnName;
            this.nullsFraction = nullsFraction;
            this.distinctValuesIndicator = distinctValuesIndicator;
            this.averageColumnLength = averageColumnLength;
        }

        public ColumnStatisticsResult(String columnName, Optional<Float> distinctValuesIndicator, Optional<Integer> averageColumnLength)
        {
            this.columnName = columnName;
            this.nullsFraction = Optional.empty();
            this.distinctValuesIndicator = distinctValuesIndicator;
            this.averageColumnLength = averageColumnLength;
        }

        public String getColumnName()
        {
            return columnName;
        }

        public Optional<Float> getNullsFraction()
        {
            return nullsFraction;
        }

        public Optional<Float> getDistinctValuesIndicator()
        {
            return distinctValuesIndicator;
        }

        public Optional<Integer> getAverageColumnLength()
        {
            return averageColumnLength;
        }
    }
}
