package gov.acwi.wqp.etl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.AbstractSqlPagingQueryProvider;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryUtils;
import org.springframework.util.StringUtils;

public class NwisPostgresPagingQueryProvider extends PostgresPagingQueryProvider {

    private String sortKeyTableName = "";

    @Override
    public String generateFirstPageQuery(int pageSize) {
        return generateLimitSqlQuery(false, pageSize);
    }

    @Override
    public String generateRemainingPagesQuery(int pageSize) {
        if(StringUtils.hasText(getGroupClause())) {
            return SqlPagingQueryUtils.generateLimitGroupedSqlQuery(this, true, buildLimitClause(pageSize));
        }
        else {
            return generateLimitSqlQuery(true, pageSize);
        }
    }

    private String generateLimitSqlQuery(boolean remainingPageQuery, int pageSize) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(this.getSelectClause());
        sql.append(" FROM ").append(this.getFromClause());
        buildWhereClause(sql, remainingPageQuery);
        sql.append(" ORDER BY ").append(buildSortClause());
        sql.append(" " + buildLimitClause(pageSize));

        return sql.toString();
    }

    private String buildLimitClause(int pageSize) {
        return new StringBuilder().append("LIMIT ").append(pageSize).toString();
    }

    private void buildWhereClause(StringBuilder sql, boolean remainingPageQuery) {
        if (remainingPageQuery) {
            sql.append(" WHERE ");
            if (this.getWhereClause() != null) {
                sql.append("(");
                sql.append(this.getWhereClause());
                sql.append(") AND ");
            }

            SqlPagingQueryUtils.buildSortConditions(this, sql);
        } else {
            sql.append(this.getWhereClause() == null ? "" : " WHERE " + this.getWhereClause());
        }

    }

    private String buildSortClause() {
        return SqlPagingQueryUtils.buildSortClause(this.getSortKeysWithoutAliases());
    }
}
