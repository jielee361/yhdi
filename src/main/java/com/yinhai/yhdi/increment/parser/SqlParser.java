package com.yinhai.yhdi.increment.parser;

import com.alibaba.fastjson.JSONObject;
import com.yinhai.yhdi.increment.poto.RedoObj;

public abstract class SqlParser {
    public abstract JSONObject redo2Json(RedoObj redo);
}
