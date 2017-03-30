/*jslint node: true */
"use strict";

var S = require('string'),
    _ = require('underscore');

(function (SQL) {

    // QUERY
    SQL.Query = function(){
        this.selectFieds = [];
        this.fromFields = null;
        this.selectClause = "SELECT {{fields}}";
        this.whereClause = null;
        this.orderByClause = null;
        this.groupByClause = null;
    };

    global.Sum = function(column){
        this.column = column;
        this.appendExpressions = [];
        return this;
    };

    global.Sum.prototype.toString = function(){
        return S('SUM( {{column}}) {{append}}').template({column: this.column.toString(), append: this.appendExpressions.join("")}).s;
    };

    global.Sum.prototype.append = function(expression){
        this.appendExpressions.push(expression.toString());
        return this;
    };

    global.Coalesce = function(column){
        this.column = column;
        this.appendExpressions = [];
        return this;
    };

    global.Coalesce.prototype.append = function(expression){
        this.appendExpressions.push(expression.toString());
        return this;
    };

    global.Coalesce.prototype.toString = function(){
        return S('COALESCE({{column}}, 0)  {{append}}').template({column: this.column, append: this.appendExpressions.join("")}).s;
    };

    global.NullIf = function(column, value){
        this.column = column;
        this.value = value;
        return this;
    };

    global.NullIf.prototype.toString = function(){
        return S('NULLIF({{column}}, {{value}})').template({column: this.column, value: this.value});
    };

    SQL.Query.prototype.isQuery = function() {
        return true;
    };

    SQL.Query.prototype.as = function(alias) {
        this.alias = alias;
    };

    SQL.Query.prototype.getAs = function(alias) {
        if(this.alias){
            return this.alias;
        }else{
            return "";
        }
    };  

    SQL.Query.prototype.select = function(field) {
        var fieldObject = new SQL.Field(field);
        this.selectFieds.push(fieldObject);
        return fieldObject;
    };

    SQL.Query.prototype.from = function(table) {
        if(!this.fromFields){
            this.fromFields = new SQL.From(table);
            return this.fromFields;
        }else{
            return this.fromFields.innerJoin(table);
        }
    };

    SQL.Query.prototype.innerJoin = function(table) {
        if (!this.fromFields){
            return this.from(table); 
        }
        return this.fromFields.innerJoin(table);
    };
    
    SQL.Query.prototype.leftJoin = function(table) {
        if (!this.fromFields){
            return this.from(table); 
        }
        return this.fromFields.leftJoin(table);
    };

    SQL.Query.prototype.rightJoin = function(table) {
        if (!this.fromFields){
            return this.from(table); 
        }
        return this.fromFields.rightJoin(table);
    };

    SQL.Query.prototype.where = function(clauseLeft){
        if (!this.whereClause){
            this.whereClause = new SQL.Where();
        }
        return this.whereClause.and(clauseLeft);
    };

    SQL.Query.prototype.groupBy = function(field){
        if(!this.groupByClause){
            this.groupByClause = new SQL.GroupBy();
        }
        return this.groupByClause.and(field);
    };

    SQL.Query.prototype.orderBy = function(field, asc){
        if(!this.orderByClause){
            this.orderByClause = new SQL.OrderBy();
        }
        
        return this.orderByClause.and(field, asc);
    };

    SQL.Query.prototype.makeQuery = function() {
        
        var query = S(this.selectClause).template({fields: this.selectFieds.join(",")});
        query += this.fromFields;
        if (this.whereClause){
            query += this.whereClause;
        }

        if(this.groupByClause){
            query += this.groupByClause;
        }

        if(this.orderByClause){
            query += this.orderByClause;
        }

        return query;
    };

    SQL.Query.prototype.toString = function() {
        return this.makeQuery();
    };


    // FIELDS
    SQL.Field = function(field){
        this.field = field;
    };

    SQL.Field.prototype.as = function(fieldName) {
        this.fieldName = fieldName;
    };

    SQL.Field.prototype.toString = function(fieldName) {
        if (this.fieldName){
            return S("{{field}} AS {{fieldName}}").template({field: this.field,fieldName: this.fieldName}).s;
        }else{
            return S("{{field}}").template({field: this.field}).s;
        }
    };


    // FROM
    SQL.From = function(table){
        this.table = table;
        this.joins = [];
        if (table.isQuery){
            this.isQuery = table.isQuery();
        }
    };

    SQL.From.prototype.as = function(alias){
        this.alias = alias;
        if (this.isQuery){
            this.table.as(alias);
        }
        return this;
    };

    SQL.From.prototype.innerJoin = function(table){
        var innerJoin = new SQL.InnerJoin(table);
        this.joins.push(innerJoin);
        return innerJoin;
    };

    SQL.From.prototype.leftJoin = function(table){
        var innerJoin = new SQL.LeftJoin(table);
        this.joins.push(innerJoin);
        return innerJoin;
    };

    SQL.From.prototype.rightJoin = function(table){
        var innerJoin = new SQL.RightJoin(table);
        this.joins.push(innerJoin);
        return innerJoin;
    };


    SQL.From.prototype.toString = function(){

        if (this.isQuery){
            if (this.joins){
                return S(" FROM ({{table}}) {{alias}} {{joins}}").template({table: this.table, alias: this.table.getAs(), joins: this.joins.join("")}).s;
            }else{
                return S(" FROM ({{table}}) {{alias}}").template({table: this.table, alias: this.table.getAs()}).s;
            }
        }else{
            if(this.alias){
                if (this.joins){
                    return S(" FROM ({{table}}) {{alias}} {{joins}}").template({table: this.table, alias: this.alias, joins: this.joins.join("")}).s;
                }else{
                    return S(" FROM ({{table}}) {{alias}} ").template({table: this.table, alias: this.alias}).s;
                }
            }else{
                if (this.joins){
                    return S(" FROM {{table}} {{joins}}").template({table: this.table, joins: this.joins.join("")}).s;
                }else{
                    return S(" FROM {{table}} ").template({table: this.table}).s;
                }
            }
        }
    };

    // JOINS
    SQL.Join = function(type, table){
        if(!this.tableRight){
            this.tableRight = table;
            this.onFields = [];
            this.alias = "";
        }
        this.type = type;
    };

    SQL.Join.prototype.on = function(whereClause){
        var clause = new SQL.Clause(whereClause, this);
        this.onFields.push(clause);
        return clause;
    };

    SQL.Join.prototype.as = function(alias){
        this.alias = alias
        return this;
    };  

    SQL.Join.prototype.and = function(otherWhereClause){
        return this.on(otherWhereClause);
    };

    SQL.Join.prototype.toString = function(){
        return S(" {{type}} JOIN {{table}} {{alias}} ON {{on}} ").template({type: this.getType(), table: this.tableRight, alias: this.alias, on: this.onFields.join(" AND ")}).s;
    };

    SQL.Join.prototype.getType = function(){
        if (this.type === 'inner'){
            return " INNER ";
        }else if (this.type === 'left'){
            return " LEFT ";
        }else if (this.type === 'right'){
            return " RIGHT";
        }else{
            return "";
        }
    };

    // inner join
    SQL.InnerJoin = function(table){
        var join = new SQL.Join("inner", table);
        _.extend(this, join);
        return this;
    };

    // left join
    SQL.LeftJoin = function(table){
        var join = new SQL.Join("left", table);
        _.extend(this, join);
        return this;
    };

    // right join
    SQL.RightJoin = function(table){
        var join = new SQL.Join("left", table);
        _.extend(this, join);
        return this;
    };

    // WHERE
    SQL.Where = function(clauseLeft){
        if (!this.clauses){
            this.clauses = [];
        }
        if (clauseLeft){
            this.and(clauseLeft);           
        }
    };

    SQL.Where.prototype.and = function(clauseLeft){
        var clauseObj = new SQL.Clause(clauseLeft, this);
        this.clauses.push(clauseObj);
        return clauseObj;
    };

    SQL.Where.prototype.toString = function(){
        return S(" WHERE {{clauses}} ").template({clauses: this.clauses.join(" AND ")}).s;
    };


    // CLAUSE
    SQL.Clause = function(clauseLeft, whereClause){
        this.clauseLeft = clauseLeft;
        this.whereClause = whereClause;
    };

    SQL.Clause.prototype.equals = function(clauseRight){
        this.clauseRight = clauseRight;
        this.clauseType = " = ";
        return this;
    };

    SQL.Clause.prototype.like = function(clauseRight){
        this.clauseRight = clauseRight;
        this.clauseType = " like ";
        return this;
    };

    SQL.Clause.prototype.lower = function(clauseRight){
        this.clauseRight = clauseRight;
        this.clauseType = " < ";
        return this;
    };

    SQL.Clause.prototype.upper = function(clauseRight){
        this.clauseRight = clauseRight;
        this.clauseType = " > ";
        return this;
    };

    SQL.Clause.prototype.different = function(clauseRight){
        this.clauseRight = clauseRight;
        this.clauseType = " <> ";
        return this;
    };

    SQL.Clause.prototype.lowerEquals = function(clauseRight){
        this.clauseRight = clauseRight;
        this.clauseType = " <= ";
        return this;
    };

    SQL.Clause.prototype.upperEquals = function(clauseRight){
        this.clauseRight = clauseRight;
        this.clauseType = " >= ";
        return this;
    };

    SQL.Clause.prototype.notEquals = function(clauseRight){
        this.clauseRight = clauseRight;
        this.clauseType = " <> ";
        return this;
    };

    SQL.Clause.prototype.is = function(clauseRight){
        this.clauseRight = clauseRight;
        this.clauseType = " is ";
        return this;
    };

    SQL.Clause.prototype.isNot = function(clauseRight){
        this.clauseRight = clauseRight;
        this.clauseType = " is not ";
        return this;
    };

    SQL.Clause.prototype.and = function(clauseLeft){
        return this.whereClause.and(clauseLeft);
    };

    SQL.Clause.prototype.toString = function(){
        if(!this.clauseRight){
            return S(" {{from}}").template({from: this.clauseLeft}).s;
        }else{
            return S(" {{from}} {{type}} {{to}} ").template({from: this.clauseLeft, type: this.clauseType, to: this.clauseRight}).s;
        }
    };

    // GROUP BY
    SQL.GroupBy = function(field){
        this.fields = [];
        if(field){
            this.and(field);    
        }
    };

    SQL.GroupBy.prototype.and = function(field){
        this.fields.push(field);
        return this;
    };

    SQL.GroupBy.prototype.toString = function(){
        return S(" GROUP BY {{fields}}").template({fields: this.fields.join(",")}).s;
    };

    // ORDER BY
    SQL.OrderBy = function(field){
        this.fields = [];
        if(field){
            this.and(field);    
        }
    };

    SQL.OrderBy.prototype.and = function(field, asc){
        var orderFieldObj = new SQL.OrderField(field, this, asc);
        this.fields.push(orderFieldObj);
        return orderFieldObj;
    };

    SQL.OrderBy.prototype.toString = function(){
        return S(" ORDER BY {{fields}}").template({fields: this.fields.join(",")}).s;
    };

    // ORDER FIELD
    SQL.OrderField = function(field, orderBy, asc){
        this.field = field;
        this.orderBy = orderBy;
        if (this.asc !== undefined){
            this.isDesc = !asc;
        }
    };

    SQL.OrderField.prototype.asc = function(){
        this.isDesc = false;
        return this;
    };

    SQL.OrderField.prototype.desc = function(){
        this.isDesc = true;
        return this;
    };

    SQL.OrderField.prototype.and = function(field, asc){
        return this.orderBy.and(field, asc);
    };

    SQL.OrderField.prototype.toString = function(){
        var ascType = 'ASC';
        if (this.isDesc){
            ascType = 'DESC';
        }

        return S(" {{field}} {{asc}}").template({field: this.field, asc: ascType}).s;
    };

}(exports));