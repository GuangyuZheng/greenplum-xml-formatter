# greenplum-xml-formatter

使用C对Greenplum数据库进行扩展，使得Greenplum用户无需编写Transform，即可通过只读/只写外部表对XML数据进行读/写 
感谢https://github.com/dewoods/greenplum-json-formatter 的分享 

安装方法：
需要Mini-XML库

在终端中输入
make
make install
即可 

使用方法: 
create external table xmlnested(
"book.author" text,
"book.author.gender" text,
"book.title" text,
"book.year" text,
"book.price" int
)location(
'gpfdist://localhost:8080/xmlnested.xml'
)format 'custom'(
formatter=xml_formatter_import
);

create writable external table xmlnestedwrite(
"book.author" text,
"book.author.gender" text,
"book.title" text,
"book.year" text,
"book.price" int
)location(
'gpfdist://localhost:8080/xmlnested.xml'
)format 'custom'(
formatter=xml_formatter_export
);
