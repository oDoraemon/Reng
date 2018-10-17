# $todo:
# 2018-10-15
# 考虑环境变量的问题，通过指定环境来生成配置信息
# 参考Flask的环境变量方案, 看如何通过生成环境作用域

require(RMySQL)
require(mongolite)

loadConfig <- function(config_url=NULL) {
    require(configr)
    config <- NULL
    if (is.null(config_url)) {
        config <- tryCatch({
            config_url <- get("config_url", envir=globalenv(), inherits=TRUE)
            rc <- read.config(config_url, warn=FALSE)
            return(rc)
        }, error = function(e) {
            stop(e)
        })
    } else {
        config <- tryCatch({
            rc <- read.config(config_url, warn=FALSE)
            return(rc)
        }, error = function(e) {
            stop(e)
        })
    }
    return(config)
}

# my database connect
# R MGO在初始化的时候必须指定collection
# SQL的con需要手动disconnect
# 不指定collection的MGO默认为test, 与mongolite行为一致
database <- function(db, collection="test") {
    config <- loadConfig()
    config <- config[[config$config]][[db]]
    if (is.null(config)) {
        stop('Unknown database: ', db)
    }

    if (config[['db_type']] == 'mysql') {
        con <- dbConnect(MySQL(),
                         host=config[['host']],
                         port=config[['port']],
                         dbname=config[['db_name']],
                         user=config[['user']],
                         password=config[['password']]
                         )
        return(con)
    } else if (config[['db_type']] == 'mongo'){
        url <- paste("mongodb://",
                      config[['user']],
                      ":",
                      config[['password']],
                      "@",
                      config[['host']],
                      ":",
                      config[['port']],
                      "/",
                      config[['db_name']],
                      "?authMechanism=SCRAM-SHA-1", sep="")
        mgo <- mongo(collection=collection, url=url)
        return(mgo)
    } else {
        stop('Unsupported database type')
    }
}

# 参考PyMySQL的实现, insert会因为插入数据大小受限制, 因此根据字符串长度判断大小
# 目前的实现是按20000条数据为标准，应用于大表的时候存在超标的可能
# 先不管, 后期再考虑statement长度判断插入数据量
genericInsert <- function(df, target.table) {
    len.data <- nrow(df)
    begin <- 1
    end <- begin + 20000
    colname <- names(df)
    col <- paste("(`", paste(colname, collapse="`,`"), "`)", sep="")
    con <- database('ADMIN_DATABASE')
    while (begin < len.data) {
        if (end > len.data) {
            end <- len.data
        }
        val <- paste(apply(df[begin:end,], 1, function(x){
                        paste("('", paste(x, collapse="','"), "')", sep="")
                    }), collapse = ", ")

        stmt <- sprintf("INSERT INTO `%s` %s VALUES %s", target.table, col, val)
        # 因为INSERT不返回插入多少行数据, 因此如果不报错的话，以end为插入记录条数
        # 哪一步出错的话, 暂时听天由命
        dbGetQuery(con, stmt)
        begin <- end + 1
    }
    message("Finish Insert Columns: ", end)
    dbDisconnect(con)
}