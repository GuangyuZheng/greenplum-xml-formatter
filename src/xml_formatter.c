#include <string.h>
#include "jansson.h"
#include <mxml.h>

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "access/formatter.h"
#include "catalog/pg_proc.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/elog.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1( xml_formatter_import );
PG_FUNCTION_INFO_V1( xml_formatter_export );

Datum xml_formatter_import( PG_FUNCTION_ARGS );
Datum json_formatter_write( PG_FUNCTION_ARGS );

typedef struct {
    int             ncols;
    Datum           *values;
    bool            *nulls;

    char            *xml_buf;
    mxml_node_t	  *xml_root;
    int				  xml_len;
    int   			  xml_counter;

    int             rownum;
} user_read_ctx_t;

typedef struct {
    int             ncolumns;

    mxml_node_t	  *xml_root;
    mxml_node_t	  **xml_vals;

    bytea           *buf;
    Datum           *dbvalues;
    bool            *dbnulls;
} user_write_ctx_t;

Datum
xml_formatter_import( PG_FUNCTION_ARGS ) {
    HeapTuple       tuple;
    TupleDesc       tupdesc;
    MemoryContext   mc, omc;
    user_read_ctx_t      *user_ctx;
    char            *data_buf;
    int             data_cur;
    int             data_len;
    int             ncols = 0;
    int             i=0;

    if( !CALLED_AS_FORMATTER( fcinfo ) )
        elog( ERROR, "xml_formatter_import: not called by format manager" );

    tupdesc = FORMATTER_GET_TUPDESC( fcinfo );

    /* Get our internal description of the formatter */
    ncols = tupdesc->natts;
    user_ctx = (user_read_ctx_t *)FORMATTER_GET_USER_CTX( fcinfo );

    /**
     *  get our input data buf and number of valid bytes in it
     */
    data_buf = FORMATTER_GET_DATABUF( fcinfo );
    data_len = FORMATTER_GET_DATALEN( fcinfo );
    data_cur = FORMATTER_GET_DATACURSOR( fcinfo );

//    elog(NOTICE,"data_buf:%s",data_buf);

    /**
     * First call to formatter, setup context
     */
    if( user_ctx == NULL ) {
        user_ctx = palloc( sizeof( user_read_ctx_t ) );
        user_ctx->ncols = ncols;
        user_ctx->values = palloc( sizeof(Datum) * ncols );
        user_ctx->nulls = palloc( sizeof(bool) * ncols );

        user_ctx->xml_buf = palloc(sizeof(char) * data_len);
        user_ctx->xml_root = palloc(sizeof(mxml_node_t));
        user_ctx->xml_len = 0;
        user_ctx->xml_counter = 0;

        user_ctx->rownum = 0;

        FORMATTER_SET_USER_CTX( fcinfo, user_ctx );
    } else {
        user_ctx->rownum++;
    }

    /**
     * Clear column buffers
     */
    MemSet( user_ctx->values, 0, ncols * sizeof(Datum) );
    MemSet( user_ctx->nulls, false, ncols * sizeof(bool) );

    /*
     * clear xml buffers
     */
    memset(user_ctx->xml_buf,0,sizeof(char) * data_len);

    /**
     * Switch memory contexts, create tuple from data
     */
    mc = FORMATTER_GET_PER_ROW_MEM_CTX( fcinfo );
    omc = MemoryContextSwitchTo( mc );

    char c;
    int length = 0;

	/*
	 * if data_cur == data_len, it means we finished the current buffer, we will not do any formatting,
	 * instead inside forboth loop we will fall inside "if (remaining < field_size)", so there is NO need to
	 * set the BAD_ROW_DATA error string ---> there will be no formatting errors that throw exceptions
	 */
    /*This line is not finish. Next buffer will bring the remaining of the line.*/
	if (data_cur == data_len) {
		MemoryContextSwitchTo(omc);
		FORMATTER_RETURN_NOTIFICATION(fcinfo, FMT_NEED_MORE_DATA);
	}
	/**
	 * 扫描XML声明
	 */
	bool leftBeginFind = false; 				//匹配 <?
	bool rightBeginFind = false; 				//匹配 ?>
	int begin_cur = 0; 						//record the header position of XML
	while (!(leftBeginFind && rightBeginFind)) {
		if (data_cur == data_len) {
			FORMATTER_SET_DATACURSOR(fcinfo, data_cur);
			MemoryContextSwitchTo(omc);
			FORMATTER_RETURN_NOTIFICATION(fcinfo, FMT_NEED_MORE_DATA);
		}

		if (data_buf[data_cur] == ' ' || data_buf[data_cur] == '\n'
				|| data_buf[data_cur] == '\r') {
			FORMATTER_SET_DATACURSOR(fcinfo, ++data_cur);
			continue;
		}

		if (data_buf[data_cur] == '<') {
			if (data_cur < data_len - 1) {
				if (data_buf[data_cur + 1] == '?') {
					leftBeginFind = !leftBeginFind;
				}
				if (!leftBeginFind) {
					elog(ERROR, "Invalid XML header");
				} else {
					begin_cur = data_cur;
					FORMATTER_SET_DATACURSOR(fcinfo, ++data_cur);
					FORMATTER_SET_DATACURSOR(fcinfo, ++data_cur);
					length += 2;
					continue;
				}
			}
		} else if (data_buf[data_cur] == '?') {
			if (data_cur < data_len - 1)
				if (data_buf[data_cur + 1] == '>')
					rightBeginFind = !rightBeginFind;
			if (leftBeginFind && rightBeginFind) {
				length++;
				FORMATTER_SET_DATACURSOR(fcinfo, ++data_cur);
				FORMATTER_SET_DATACURSOR(fcinfo, ++data_cur);
				continue;
			} else {
				elog(ERROR, "Invalid XMl header %c", data_buf[data_cur]);
			}
		} else {
			if (!leftBeginFind) {
				elog(ERROR, "Invalid XMl header %c", data_buf[data_cur]);
			} else {
				FORMATTER_SET_DATACURSOR(fcinfo, ++data_cur);
				length += 2;
				continue;
			}
		}
	}



	/**
	 *  扫描XML的第一个元素
	 */
//	elog(NOTICE,"first letter is %c",data_buf[data_cur]);
	bool leftSymbol = false; 				//匹配 <
	bool rightSymbol = false; 				//匹配 >
	while (user_ctx->xml_counter == 0) {
		if (data_cur == data_len) {
			FORMATTER_SET_DATACURSOR(fcinfo, data_cur);
			MemoryContextSwitchTo(omc);
			FORMATTER_RETURN_NOTIFICATION(fcinfo, FMT_NEED_MORE_DATA);
		}

		if (data_buf[data_cur] == ' ' || data_buf[data_cur] == '\n'
				|| data_buf[data_cur] == '\r') {
			FORMATTER_SET_DATACURSOR(fcinfo, ++data_cur);
			continue;
		}

		if (data_buf[data_cur] == '<') {
			leftSymbol = !leftSymbol;
			if (!leftSymbol) {
				elog(ERROR, "Invalid XML object:dump <");
			} else {
				FORMATTER_SET_DATACURSOR(fcinfo, ++data_cur);
				length++;
				continue;
			}
		}
		else if (data_buf[data_cur] == '>') {
			rightSymbol = !rightSymbol;
			if (leftSymbol && rightSymbol) {
				user_ctx->xml_counter++;
				length++;
				FORMATTER_SET_DATACURSOR(fcinfo, ++data_cur);
				continue;
			} else {
				elog(ERROR, "Invalid XMl object: dump > %c", data_buf[data_cur]);
			}
		} else {
			if (!leftSymbol) {
				elog(ERROR, "Invalid XMl object:missing < %c", data_buf[data_cur]);
			} else {
				FORMATTER_SET_DATACURSOR(fcinfo, ++data_cur);
				length++;
				continue;
			}
		}
	}
	leftSymbol = false;
	rightSymbol = false;
	char *str1 = palloc(sizeof(char) * data_len);
	memcpy(str1,data_buf,data_cur);
	//elog(NOTICE,"LENGTH:%d",length);
	//elog( NOTICE, "XML object find: %s \n data_cur : %d \n", str1+'\0',data_cur);


    /**
     * 扫描到最后一个XML元组的结尾, 将所有XML元素的开头和结尾进行匹配
     */
	bool beginSymbol = false;						 //匹配 <
	bool endSymbol = false; 						 //匹配 </
	while (user_ctx->xml_counter > 0) {
		/**
		 * Make sure we have more data to scan
		 */
		if (data_cur == data_len) {
			FORMATTER_SET_DATACURSOR(fcinfo, data_cur);
			MemoryContextSwitchTo(omc);

			if (FORMATTER_GET_SAW_EOF(fcinfo)) {
				FORMATTER_SET_BAD_ROW_NUM(fcinfo, user_ctx->rownum);
				FORMATTER_SET_BAD_ROW_DATA(fcinfo, data_buf + begin_cur,
						length);
				ereport(ERROR,
						( errcode( ERRCODE_DATA_EXCEPTION ), errmsg( "Invalid XML object user_ctx->xml_counter: %d begin_cur: %d, length: %d, user_ctx->rownum: %d data_len: %d data_buf+data_cur: %s", user_ctx->xml_counter, begin_cur, length, user_ctx->rownum, data_len , data_buf+begin_cur) ));
			} else {
				FORMATTER_RETURN_NOTIFICATION(fcinfo, FMT_NEED_MORE_DATA);
				elog(NOTICE,"need more data");
			}
		}

		c = data_buf[data_cur];
		if (c == '<') {
			if (data_cur < data_len - 1) {
				char nextChar = data_buf[data_cur + 1];
				if (nextChar != '/')
					beginSymbol = true;
				else
					endSymbol = true;
			}
		} else if (c == '>') {
			if (beginSymbol){
				user_ctx->xml_counter++;
			}
			else if (endSymbol){
				user_ctx->xml_counter--;
			}
			beginSymbol = false;
			endSymbol = false;
		}
		FORMATTER_SET_DATACURSOR(fcinfo, ++data_cur);
		length++;
	}

    user_ctx->xml_len = data_cur-begin_cur + 1;

    memcpy( user_ctx->xml_buf, data_buf+begin_cur, user_ctx->xml_len );
    //elog(NOTICE,"DATA_CUR:%d",data_cur);
    //elog( NOTICE, "Complete str: %s", user_ctx->xml_buf);

    user_ctx->xml_root = mxmlLoadString(NULL,user_ctx->xml_buf,MXML_TEXT_CALLBACK);
    if( !user_ctx->xml_root ) {
        elog( ERROR, "Could not parse XML string" );
    }
    beginSymbol = false;
    endSymbol = false;
    user_ctx->xml_counter = 0;

    /**
     * Pull each database column from the XML object
     */
    for( i=0; i < ncols; i++ ) {
        Oid         type    = tupdesc->attrs[i]->atttypid;
        mxml_node_t      *val;
        char        *dbcolname, *tofree;
        char        *objname;

        /**
         * copy the name of the current database column, to be passed to strsep
         */
        tofree = dbcolname = strdup( tupdesc->attrs[i]->attname.data );

        /**
         * Initially set to the root of the current XML object
         */
        val = palloc( sizeof(mxml_node_t) );
        val = user_ctx->xml_root;

        /**
         * 如果表中字段包含符号. 则需要遍历XML树寻找对应的子元素
         */
        bool this_obj_is_null = false;

		while ((objname = strsep(&dbcolname, "."))) {
			val = mxmlFindElement(val, user_ctx->xml_root, objname, NULL, NULL,
					MXML_DESCEND);
//			elog(NOTICE,"get a val:%s",mxmlGetElement(val));
			if (!val) {
				this_obj_is_null = true;
				elog(NOTICE,"Obj is NULL");
				break;
			}
		}
		if (this_obj_is_null) {
			val = mxmlNewElement(user_ctx->xml_root,"null");
		}

		free(tofree);
//		elog(NOTICE,"finally get a val:%s",mxmlGetElement(val));
        /**
         * 根据表中字段的类型提取出相应元素的值
         */
		switch (type) {
		case INT2OID:
		case INT4OID:
		case INT8OID: {
			if (!strcmp(mxmlGetElement(val), "null")) {
				elog(NOTICE,"NULL");
				user_ctx->nulls[i] = true;
			} else {
				char* value = val->child->value.text.string;
//				elog(NOTICE,"value:%s",value+'\0');
				user_ctx->values[i] = (Datum) atoi(value);
				user_ctx->nulls[i] = false;
			}
			break;
		}
		case FLOAT4OID: {
			if (!strcmp(mxmlGetElement(val), "null")) {
				user_ctx->nulls[i] = true;
			} else {
				char* value = val->child->value.text.string;
				user_ctx->values[i] = Float4GetDatum(atof(value));
				user_ctx->nulls[i] = false;
			}
			break;
		}
		case FLOAT8OID: {
			if (!strcmp(mxmlGetElement(val), "null")) {
				user_ctx->nulls[i] = true;
			} else {
				char* value = val->child->value.text.string;
				user_ctx->values[i] = Float8GetDatum(atof(value));
				user_ctx->nulls[i] = false;
			}
			break;
		}
		case TEXTOID:
		case VARCHAROID: {
			char *strval;
			text *txtval;
			if (!strcmp(mxmlGetElement(val), "null")) {
				user_ctx->nulls[i] = true;
			} else {
				strval = val->child->value.text.string;
				txtval = palloc(strlen( strval ) + VARHDRSZ);
				SET_VARSIZE(txtval, strlen(strval) + VARHDRSZ);
				memcpy(VARDATA(txtval), strval, strlen(strval));

				user_ctx->values[i] = PointerGetDatum(txtval);
				user_ctx->nulls[i] = false;
			}
			break;
		}
		default: {
			MemoryContextSwitchTo(omc);
			FORMATTER_SET_BAD_ROW_NUM(fcinfo, user_ctx->rownum);
			FORMATTER_SET_BAD_ROW_DATA(fcinfo, user_ctx->xml_buf,
					user_ctx->xml_len);
			ereport(ERROR,
					( errcode( ERRCODE_DATA_EXCEPTION ), errmsg( "Unsupported data type '%d' for column '%s'", type, tupdesc->attrs[i]->attname.data ) ));
		}
        }
    }

    MemoryContextSwitchTo( omc );
    FORMATTER_SET_DATACURSOR( fcinfo, data_cur);

    tuple = heap_form_tuple( tupdesc, user_ctx->values, user_ctx->nulls );

    FORMATTER_SET_TUPLE( fcinfo, tuple );
    FORMATTER_RETURN_TUPLE( tuple );
}

Datum xml_formatter_export( PG_FUNCTION_ARGS) {
	HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
	TupleDesc tupdesc;
	HeapTupleData tuple;
	MemoryContext mc, omc;
	user_write_ctx_t *user_ctx;
	char *xbuf;
	int xbufn;
	int ncolumns = 0;
	int i = 0;

	/**
	 * Must be called via the ext tab format manager
	 */
	if (!CALLED_AS_FORMATTER(fcinfo))
		elog(ERROR, "xml_formatter_export: not called by format manager");

	tupdesc = FORMATTER_GET_TUPDESC(fcinfo);
	ncolumns = tupdesc->natts;

	user_ctx = (user_write_ctx_t *) FORMATTER_GET_USER_CTX(fcinfo);

	/**
	 * First call to formatter, setup context
	 */
	if (user_ctx == NULL) {
		user_ctx = palloc(sizeof(user_write_ctx_t));

		user_ctx->ncolumns = ncolumns;

		user_ctx->xml_root = mxmlNewXML("1.0");
		user_ctx->xml_vals = palloc(sizeof(mxml_node_t*) * ncolumns);

		user_ctx->buf = palloc(VARHDRSZ);
		user_ctx->dbvalues = palloc(sizeof(Datum) * ncolumns);
		user_ctx->dbnulls = palloc(sizeof(bool) * ncolumns);

		for (i = 0; i < ncolumns; i++) {
			Oid type = tupdesc->attrs[i]->atttypid;
			char *dbcolname, *tofree;
			char *objname, *pobjname;
			mxml_node_t *xml_parent = user_ctx->xml_root;

			tofree = dbcolname = strdup(tupdesc->attrs[i]->attname.data);
			user_ctx->xml_vals[i] = NULL;
			/**
			 * Deal with column names containing the nested seperator
			 */
			while ((objname = strsep(&dbcolname, ".")) != NULL) {
				if (user_ctx->xml_vals[i])
					xml_parent = user_ctx->xml_vals[i];

				//nested objects may already exist, add only if needed
				user_ctx->xml_vals[i] = mxmlFindElement(xml_parent,
						user_ctx->xml_root, objname, NULL, NULL, MXML_DESCEND);
				if (!user_ctx->xml_vals[i]) {
					//generic objects will be replaced later if necessary
					user_ctx->xml_vals[i] = mxmlNewElement(xml_parent,objname);
					if (!user_ctx->xml_vals[i]) {
						elog(ERROR, "Failed to append nested XML object");
					}
				}

				//keep a pointer to the current sub-object name
				pobjname = objname;
			}

			switch (type) {
			case INT2OID:
			case INT4OID:
			case INT8OID: {
				mxml_node_t *result = mxmlNewInteger(user_ctx->xml_vals[i], 0);
				if (result == NULL) {
					elog(ERROR, "Could not initialize XML integer");
				}

				break;
			}
			case FLOAT4OID:
			case FLOAT8OID: {
				mxml_node_t *result = mxmlNewReal(user_ctx->xml_vals[i], 0.0);
				if (result == NULL) {
					elog(ERROR, "Could not initialize XML real");
				}

				break;
			}
			case TEXTOID:
			case VARCHAROID: {
				mxml_node_t *result = mxmlNewText(user_ctx->xml_vals[i], 1, "");
				if (result == NULL) {
					elog(ERROR, "Could not initialize XML string");
				}

				break;
			}
			default: {
				elog(ERROR, "Type not supported");
			}
			}
		}

		FORMATTER_SET_USER_CTX(fcinfo, user_ctx);
	}

	/**
	 * Clear column buffers
	 */
	MemSet(user_ctx->dbvalues, 0, ncolumns * sizeof(Datum));
	MemSet(user_ctx->dbnulls, false, ncolumns * sizeof(bool));

	/**
	 * Switch memory context
	 */
	mc = FORMATTER_GET_PER_ROW_MEM_CTX(fcinfo);
	omc = MemoryContextSwitchTo(mc);

	/**
	 * Break input record into fields
	 */
	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_data = rec;
	heap_deform_tuple(&tuple, tupdesc, user_ctx->dbvalues, user_ctx->dbnulls);

	/**
	 * Iterate over columns from database
	 */
	for (i = 0; i < ncolumns; i++) {
		Oid type = tupdesc->attrs[i]->atttypid;
		int ret = 0;

		switch (type) {
		case INT2OID:
		case INT4OID:
		case INT8OID: {
			int64 value;

			if (user_ctx->dbnulls[i])
				value = 0;
			else
				value = DatumGetInt64(user_ctx->dbvalues[i]);

			ret = mxmlSetInteger(user_ctx->xml_vals[i],value);
			if (ret < 0) {
				MemoryContextSwitchTo(omc);
				elog(ERROR, "Unable to set int value for column '%s'",
						tupdesc->attrs[i]->attname.data);
			}
			break;
		}
		case FLOAT4OID: {
			float4 value;

			if (user_ctx->dbnulls[i])
				value = 0;
			else
				value = DatumGetFloat4(user_ctx->dbvalues[i]);

			ret = mxmlSetReal(user_ctx->xml_vals[i],(double)value);
			if (ret < 0) {
				MemoryContextSwitchTo(omc);
				elog(ERROR, "Unable to set float value for column '%s'",
						tupdesc->attrs[i]->attname.data);
			}
			break;
		}
		case FLOAT8OID: {
			float8 value;

			if (user_ctx->dbnulls[i])
				value = 0;
			else
				value = DatumGetFloat8(user_ctx->dbvalues[i]);

			ret = mxmlSetReal(user_ctx->xml_vals[i],(double)value);
			if (ret < 0) {
				MemoryContextSwitchTo(omc);
				elog(ERROR, "Unable to set float value for column '%s'",
						tupdesc->attrs[i]->attname.data);
			}
			break;
		}
		case TEXTOID:
		case VARCHAROID: {
			char *value;

			if (user_ctx->dbnulls[i]) {
				value = "";
			} else {
				value = DatumGetCString(
						DirectFunctionCall1(textout, user_ctx->dbvalues[i]));
			}

			ret = mxmlSetText(user_ctx->xml_vals[i],1, value);
			if (ret < 0) {
				MemoryContextSwitchTo(omc);
				elog(ERROR, "Unable to set string value for column '%s'",
						tupdesc->attrs[i]->attname.data);
			}

			break;
		}
		default: {
			MemoryContextSwitchTo(omc);
			elog(ERROR, "Type of column '%s' not supported",
					tupdesc->attrs[i]->attname.data);
		}
		}
	}

	/**
	 * 以字符串的形式输出XML数据
	 */
	xbuf = mxmlSaveAllocString(user_ctx->xml_root,MXML_NO_CALLBACK);
	xbufn = strlen(xbuf);
	/**
	 * Copy the resulting string into our bytea buffer with a trailing newline
	 */
	MemoryContextSwitchTo(omc);

	if (user_ctx->buf)
		pfree(user_ctx->buf);

	user_ctx->buf = palloc(xbufn + VARHDRSZ + 1);
	char *data = VARDATA(user_ctx->buf);

	SET_VARSIZE(user_ctx->buf, xbufn + VARHDRSZ + 1);
	memcpy(VARDATA(user_ctx->buf), xbuf, xbufn);
	memcpy(&data[xbufn], "\n", 1);
	free(xbuf);

	PG_RETURN_BYTEA_P(user_ctx->buf);
}
