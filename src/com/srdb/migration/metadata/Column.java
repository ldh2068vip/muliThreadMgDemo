package com.srdb.migration.metadata;


public class Column {

    private String columnName = null;//列名
    private String dataTypeStr = null;//数据类型，数据库查出来的
    private DataType dataType = null;//枚举类型值
    private int dataLength = 0;//数据长度，只对字符类型有效
    private int dataPrecision = 0;//数字类型整数位数
    private int dataScale = 0;//数字类型小数点后位数
    private boolean nullable = false;//是否为空
    private String defaultClause = null;//默认值
    private String comments = null;//列备注
    private Table parentTable = null;//所在表名

    public Column() {

    }

    public Column(String ColumnName) {
        this.columnName = ColumnName;
    }

    public Column(Table parentTable, String ColumnName) {
        this.parentTable = parentTable;
        this.columnName = ColumnName;
    }

    /**
     * @param DataTypeStr
     * @param DataLength
     * @param DataPrecision
     * @param DataScale
     * @param Nullable
     * @param defaultClause
     * @param comments
     */
    public void addDetail(String DataTypeStr, int DataLength, int DataPrecision, int DataScale, String Nullable, String defaultClause, String comments) {

        this.dataTypeStr = DataTypeStr;
        this.dataLength = DataLength;
        this.dataScale = DataScale;
        this.dataPrecision = DataPrecision;
        if ((Nullable.compareToIgnoreCase("Y") == 0)
                || (Nullable.compareToIgnoreCase("YES") == 0)
                || (Nullable.compareToIgnoreCase("1") == 0)) {
            this.nullable = true;
        } else {
            this.nullable = false;
        }
        this.defaultClause = defaultClause;
        this.comments = comments;

        if (isInterval()) {
            this.dataType = DataType.INTERVAL;

        } else if (isBit()) {
            this.dataType = DataType.BIT;

        } else if (isBitString()) {
            this.dataType = DataType.BITSTRING;
        } else if (isVarchar()) {
            this.dataType = DataType.VARCHAR;

        } else if (isNVarchar()) {
            this.dataType = DataType.NVARCHAR;

        } else if (isText()) {
            this.dataType = DataType.TEXT;
        } else if (isDate()) {
            this.dataType = DataType.DATE;
        } else if (isNumeric()) {
            if (isFloat())
                this.dataType = DataType.FLOAT;
            else if (isBinaryFloat())
                this.dataType = DataType.BINARY_FLOAT;
            else if (isInteger())
                this.dataType = DataType.INTEGER;
            else
                this.dataType = DataType.NUMERIC;
        } else if (isTime()) {
            this.dataType = DataType.TIME;
        } else if (isTimeStamp()) {
            this.dataType = DataType.TIMESTAMP;
        } else if (isLargeObject()) {
            this.dataType = DataType.BYTEA;
        } else if (isRowID()) {
            this.dataType = DataType.ROWID;
        } else if (isBoolean()) {
            this.dataType = DataType.BOOLEAN;
        } else if (isMoney()) {
            this.dataType = DataType.MONEY;
        } else if (isXML()) {
            this.dataType = DataType.XML;
        } else if (isGeometric()) {
            this.dataType = DataType.GEOMETRIC;
        } else if (isNetworkAddress()) {
            this.dataType = DataType.NETWORKADDRESS;
        } else if (isOID()) {
            this.dataType = DataType.OID;
        } else if (isNAME()) {
            this.dataType = DataType.NAME;
        } else if (isUserDefined()) {
            this.dataType = DataType.USERDEFINED;
        } else if (isGUID()) {
            this.dataType = DataType.GUID;
        } else {
            this.dataType = DataType.OTHER;
        }

    }

    public boolean isBit() {
        return this.dataTypeStr.equalsIgnoreCase("BIT");
    }

    public boolean isVarchar() {
        return (this.dataTypeStr.compareToIgnoreCase("VARCHAR2") == 0)
                || (this.dataTypeStr.compareToIgnoreCase("VARCHAR") == 0)
                || (this.dataTypeStr.compareToIgnoreCase("CHAR") == 0)
                || (this.dataTypeStr.equalsIgnoreCase("CHARACTER"))
                || (this.dataTypeStr.equalsIgnoreCase("CHARACTER VARYING"));
    }

    public boolean isNVarchar() {
        return (this.dataTypeStr.compareToIgnoreCase("NVARCHAR2") == 0)
                || (this.dataTypeStr.compareToIgnoreCase("NCHAR") == 0);
    }

    public boolean isText() {
        return
                //mod by kevin
                (this.dataTypeStr.compareToIgnoreCase("LONG") == 0)
                        || (this.dataTypeStr.compareToIgnoreCase("LONG VARCHAR") == 0)
                        ||

                        (this.dataTypeStr.compareToIgnoreCase("CLOB") == 0)
                        || (this.dataTypeStr.compareToIgnoreCase("NCLOB") == 0)
                        || (this.dataTypeStr.equalsIgnoreCase("TEXT"))
                        || (this.dataTypeStr.compareToIgnoreCase("VARGRAPHIC") == 0);
    }

    public boolean isNumeric() {
        return (this.dataTypeStr.equalsIgnoreCase("NUMBER"))
                || (this.dataTypeStr.equalsIgnoreCase("NUMERIC"))
                || (this.dataTypeStr.toUpperCase().startsWith("FLOAT"))
                || (this.dataTypeStr.equalsIgnoreCase("DOUBLE"))
                || (this.dataTypeStr.equalsIgnoreCase("DECIMAL"))
                || (this.dataTypeStr.equalsIgnoreCase("INTEGER"))
                || (this.dataTypeStr.equalsIgnoreCase("INT"))
                || (this.dataTypeStr.equalsIgnoreCase("SMALLINT"))
                || (this.dataTypeStr.equalsIgnoreCase("BIT"))
                || (this.dataTypeStr.equalsIgnoreCase("TINYINT"))
                || (this.dataTypeStr.equalsIgnoreCase("MEDIUMINT"))
                || (this.dataTypeStr.equalsIgnoreCase("BIGINT"))
                || (this.dataTypeStr.equalsIgnoreCase("REAL"))
                || (this.dataTypeStr.equalsIgnoreCase("DOUBLE PRECISION"))
                || (this.dataTypeStr.equalsIgnoreCase("YEAR"))
                || (this.dataTypeStr.equalsIgnoreCase("SERIAL"))
                || (this.dataTypeStr.equalsIgnoreCase("BIGSERIAL"))
                || (this.dataTypeStr.equalsIgnoreCase("BINARY_DOUBLE"))
                || (this.dataTypeStr.equalsIgnoreCase("BINARY_FLOAT"));
    }

    public boolean isFloat() {
        return (this.dataTypeStr.toUpperCase().startsWith("FLOAT"))
                || (this.dataTypeStr.equalsIgnoreCase("DOUBLE PRECISION"))
                || (this.dataTypeStr.equalsIgnoreCase("REAL"))
                || (this.dataTypeStr.equalsIgnoreCase("DOUBLE"))
                || (this.dataTypeStr.equalsIgnoreCase("DECIMAL"))
                || (this.dataTypeStr.equalsIgnoreCase("BINARY_DOUBLE"));
    }

    public boolean isBinaryFloat() {
        return this.dataTypeStr.equalsIgnoreCase("BINARY_FLOAT");
    }

    public boolean isInteger() {
        return (isNumeric()) && ((this.dataScale == 0) || (isSmallint()));
    }

    public boolean isDate() {
        return this.dataTypeStr.compareToIgnoreCase("DATE") == 0;
    }

    public boolean isTime() {
        return (this.dataTypeStr.compareToIgnoreCase("TIME") == 0)
                || (this.dataTypeStr.equalsIgnoreCase("TIME WITHOUT TIME ZONE"))
                || (this.dataTypeStr.equalsIgnoreCase("TIME WITH TIME ZONE"));
    }

    public boolean isTimeStamp() {
        if ((this.dataTypeStr.equalsIgnoreCase("DATETIME"))
                || (this.dataTypeStr.equalsIgnoreCase("DATETIME2"))
                || (this.dataTypeStr.equalsIgnoreCase("DATETIMEOFFSET"))) {
            return true;
        }

        if ((this.dataTypeStr != null)
                && (this.dataTypeStr.trim().length() >= 9)) {
            String typeName = this.dataTypeStr.trim().substring(0, 9);

            if (typeName.compareToIgnoreCase("TIMESTAMP") == 0) {
                return true;
            }
        }

        return false;
    }

    public boolean isLargeObject() {
        return (this.dataTypeStr.compareToIgnoreCase("LONG RAW") == 0)
                || (this.dataTypeStr.compareToIgnoreCase("BLOB") == 0)
                || (this.dataTypeStr.compareToIgnoreCase("RAW") == 0)
                || (this.dataTypeStr.compareToIgnoreCase("BYTEA") == 0)
                || (this.dataTypeStr.compareToIgnoreCase("BINARY") == 0)
                || (this.dataTypeStr.compareToIgnoreCase("VARBINARY") == 0)
                //add by kevin
//                || (this.dataTypeStr.compareToIgnoreCase("LONG") == 0)
//                || (this.dataTypeStr.compareToIgnoreCase("LONG VARCHAR") == 0)
                ;
    }

    public boolean isRowID() {
        return (this.dataTypeStr.equalsIgnoreCase("ROWID"))
                || (this.dataTypeStr.equalsIgnoreCase("UROWID"));
    }

    public boolean isLong() {
        return (this.dataTypeStr.compareToIgnoreCase("LONG") == 0)
                || (this.dataTypeStr.compareToIgnoreCase("LONG VARCHAR") == 0);
    }

    public boolean isInterval() {
        return (this.dataTypeStr.equalsIgnoreCase("INTERVAL"))
                || (this.dataTypeStr.toUpperCase().startsWith("INTERVAL"));
    }

    public boolean isDecimal() {
        return this.dataTypeStr.compareToIgnoreCase("DECIMAL") == 0;
    }

    public boolean isDouble() {
        return (this.dataTypeStr.toUpperCase().startsWith("DOUBLE"))
                || (this.dataTypeStr.equalsIgnoreCase("BINARY_DOUBLE"));
    }

    public boolean isReal() {
        return this.dataTypeStr.toUpperCase().startsWith("REAL");
    }

    public boolean isSmallint() {
        return (this.dataTypeStr.equalsIgnoreCase("SMALLINT"))
                || (this.dataTypeStr.equalsIgnoreCase("TINYINT"));
    }

    public boolean isBlob() {
        return this.dataTypeStr.equalsIgnoreCase("BLOB");
    }

    public boolean isLongRaw() {
        return this.dataTypeStr.equalsIgnoreCase("LONG RAW");
    }

    public boolean isClob() {
        return this.dataTypeStr.equalsIgnoreCase("CLOB");
    }

    public boolean isNClob() {
        return this.dataTypeStr.equalsIgnoreCase("NCLOB");
    }

    public boolean isWholeNumberType() {
        return (this.dataTypeStr.equalsIgnoreCase("INTEGER"))
                || (this.dataTypeStr.equalsIgnoreCase("BIGINT"))
                || (this.dataTypeStr.equalsIgnoreCase("SERIAL"))
                || (this.dataTypeStr.equalsIgnoreCase("BIGSERIAL"))
                || (isSmallint());
    }

    public boolean isBoolean() {
        return this.dataTypeStr.equalsIgnoreCase("BOOLEAN");
    }

    public boolean isMoney() {
        return (this.dataTypeStr.equalsIgnoreCase("MONEY"))
                || (this.dataTypeStr.equalsIgnoreCase("SMALLMONEY"));
    }

    public boolean isXML() {
        return (this.dataTypeStr.equalsIgnoreCase("XML"))
                || (this.dataTypeStr.equalsIgnoreCase("XMLTYPE"));
    }

    public boolean isGeometric() {
        return (this.dataTypeStr.equalsIgnoreCase("POINT"))
                || (this.dataTypeStr.equalsIgnoreCase("LINE"))
                || (this.dataTypeStr.equalsIgnoreCase("LSEG"))
                || (this.dataTypeStr.equalsIgnoreCase("BOX"))
                || (this.dataTypeStr.equalsIgnoreCase("PATH"))
                || (this.dataTypeStr.equalsIgnoreCase("POLYGON"))
                || (this.dataTypeStr.equalsIgnoreCase("CIRCLE"));
    }

    public boolean isBitString() {
        return (this.dataTypeStr.equalsIgnoreCase("BIT VARYING"))
                || (this.dataTypeStr.equalsIgnoreCase("BIT"));
    }

    public boolean isNetworkAddress() {
        return (this.dataTypeStr.equalsIgnoreCase("CIDR"))
                || (this.dataTypeStr.equalsIgnoreCase("INET"))
                || (this.dataTypeStr.equalsIgnoreCase("MACADDR"));
    }

    public boolean isOID() {
        return this.dataTypeStr.equalsIgnoreCase("OID");
    }

    public boolean isNAME() {
        return this.dataTypeStr.equalsIgnoreCase("NAME");
    }

    public boolean isUserDefined() {
        return this.dataTypeStr.equalsIgnoreCase("USER-DEFINED");
    }

    public boolean isGUID() {
        return (this.dataTypeStr.equalsIgnoreCase("UUID"))
                || (this.dataTypeStr.equalsIgnoreCase("UNIQUEIDENTIFIER"));
    }


    public String getName() {
        return this.columnName;
    }

    public static enum DataType {
        VARCHAR, NVARCHAR, INTEGER, NUMERIC, FLOAT, DATE, TIME, TIMESTAMP, TEXT, BYTEA, ROWID, INTERVAL, BIT, BOOLEAN, MONEY, XML, ARRAY, GEOMETRIC, BITSTRING, NETWORKADDRESS, OID, NAME, USERDEFINED, GUID, BINARY_FLOAT, OTHER;
    }

    //getter and setter


    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public int getDataLength() {
        return dataLength;
    }

    public void setDataLength(int dataLength) {
        this.dataLength = dataLength;
    }

    public int getDataPrecision() {
        return dataPrecision;
    }

    public void setDataPrecision(int dataPrecision) {
        this.dataPrecision = dataPrecision;
    }

    public int getDataScale() {
        return dataScale;
    }

    public void setDataScale(int dataScale) {
        this.dataScale = dataScale;
    }

    public String getDataTypeStr() {
        return dataTypeStr;
    }

    public void setDataTypeStr(String dataTypeStr) {
        this.dataTypeStr = dataTypeStr;
    }

    public String getDefaultClause() {
        return defaultClause;
    }

    public void setDefaultClause(String defaultClause) {
        this.defaultClause = defaultClause;
    }


    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public Table getParentTable() {
        return parentTable;
    }

    public void setParentTable(Table parentTable) {
        this.parentTable = parentTable;
    }

    public DataType getDataType() {
        return dataType;
    }
}
