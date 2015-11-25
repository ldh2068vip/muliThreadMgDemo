package com.srdb.migration.metadata;


import org.apache.log4j.Logger;

public class Table {
	private static Logger logger = Logger.getLogger(Table.class.getName());
	private String getColumnSqlStr = "SELECT a.data_default, a.column_name, a.data_type, a.data_length, a.data_precision, a.data_scale, a.nullable, b.comments, a.char_used, a.char_length FROM all_tab_columns a, all_col_comments b WHERE a.owner = ? and a.table_name = ? AND a.owner = b.owner AND a.table_name = b.table_name AND a.column_name = b.column_name ";
	private String tableName = null; //表名
	private ColumnList columns = null;//列集合
	private String parentSchema = null;//模式名
	public Table() {
	}

	public Table(String parentSchema, String tableName) {
		this.parentSchema = parentSchema;
		this.tableName = tableName;
	}

	public String getName() {
		return this.tableName;
	}

	public void addColumn(Column column) {

		if (this.columns == null) {
			this.columns = new ColumnList();
		}
		this.columns.add(column);

	}

	public int getLargeObjectsColumnCount() {
		int lobjColCount = 0;

		for (int i = 0; i < this.columns.size(); i++) {
			if (this.columns.get(i).isLargeObject()) {
				lobjColCount++;
			}
		}

		return lobjColCount;
	}

	public String getTargetSchemaQualifiedName() {

		return this.parentSchema + "." + this.tableName;
	}

	public String getCreateScript(Table table) {

		String sql = "CREATE UNLOGGED TABLE " + table.getTargetSchemaQualifiedName() + " (\n";

		if (table.getColumns() != null) {
			for (int i = 0; i < table.getColumns().size(); i++) {
				sql = sql + getColumnLine(table.getColumns().get(i));
				if (i < table.getColumns().size() - 1)
					sql = sql + ", ";
				sql = sql + "\n";
			}

		}

		sql = sql + ")";
		logger.debug("建表语句： " + sql);
		return sql;
	}

	public String getCreateSchemaScript(String schemaName) {
		String sql = "CREATE SCHEMA " + schemaName + ";\n";
		logger.debug("模式创建语句："+sql);
		return sql;
	}

	protected String getColumnLine(Column column) {
		String str = "";
		String colName = column.getName();

		if ((colName.indexOf('#') >= 0)) {
			colName = "\"" + colName.toLowerCase() + "\"";
		}

		str = str +  colName;

		String customType = null;

		if (column.isRowID()) {

			str = str + " " + "VARCHAR";
		} else if (column.isInterval())
			str = str + " " + "INTERVAL";
		else if (column.isTimeStamp()) {
			str = str + " "
					+ column.getDataTypeStr().replaceAll("LOCAL", "");
		} else if ((column.isNumeric())
				&& (column.getDataTypeStr().equalsIgnoreCase("NUMBER") || column
				.isBinaryFloat())) {
			str = str + " NUMERIC";
		} else if ((column.isNumeric())
				&& (column.getDataTypeStr().equalsIgnoreCase("DEC"))) {
			str = str + " DECIMAL";
		} else if ((column.isVarchar())
				&& ((column.getDataTypeStr()
				.equalsIgnoreCase("VARCHAR2")) || (column
				.getDataTypeStr()
				.equalsIgnoreCase("CHAR VARYING")))) {
			str = str + " VARCHAR";
		} else if (column.isText())
			str = str + " TEXT";
		else if (column.isLargeObject())
			str = str + " BYTEA";
		else {
			str = str + " " + column.getDataTypeStr();
		}

			if ((column.getDataType() == Column.DataType.VARCHAR)
					|| (column.getDataType() == Column.DataType.NVARCHAR)) {
				str = str + "(" + column.getDataLength() + ")";
			}
			//datalength 只对字符类型有效

			if (((column.getDataType() == Column.DataType.INTEGER) || (column
					.getDataType() == Column.DataType.NUMERIC))
					&& (column.getDataPrecision() > 0)) {
				str = str + "(" + column.getDataPrecision();
				if (column.getDataScale() > 0)
					str = str + "," + column.getDataScale();
				str = str + ")";
			}


		if (column.getDefaultClause() != null) {

			str = str + " DEFAULT " + column.getDefaultClause();
		}

		if (!column.isNullable()) {
			str = str + " NOT NULL";
		}
		return str;
	}




	//setter and getter
	public ColumnList getColumns() {
		return columns;
	}

	public String getParentSchema() {
		return parentSchema;
	}

	public String getGetColumnSqlStr() {
		return getColumnSqlStr;
	}
}