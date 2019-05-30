package gov.acwi.wqp.etl;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.item.database.Order;

public class NwisPostgresPagingQueryProviderTest {

    protected NwisPostgresPagingQueryProvider queryProvider;

    private final static String SELECT_CLAUSE = "t1.a, t1.b, t2.c";
    private final static String FROM_CLAUSE = "t1 join t2 on t1.a = t2.a";
    private final static String WHERE_CLAUSE = "t1.flag is not null";

    @Before
    public void setup() {
        Map<String, Order> sortKeys = new HashMap<>();

        queryProvider = new NwisPostgresPagingQueryProvider();
        queryProvider.setSelectClause(SELECT_CLAUSE);
        queryProvider.setFromClause(FROM_CLAUSE);
        queryProvider.setWhereClause(WHERE_CLAUSE);
        sortKeys.put("t1.a", Order.ASCENDING);
        queryProvider.setSortKeys((sortKeys));
    }

    @Test
    public void generateFirstPageQueryTest() {
        assertEquals("SELECT " + SELECT_CLAUSE + " FROM " + FROM_CLAUSE + " WHERE " + WHERE_CLAUSE + " ORDER BY a ASC LIMIT 10", queryProvider.generateFirstPageQuery(10));
    }

    @Test
    public void generateRemainingPagesQueryTest() {
        queryProvider.generateFirstPageQuery(10);
        assertEquals("SELECT " + SELECT_CLAUSE + " FROM " + FROM_CLAUSE + " WHERE (" + WHERE_CLAUSE + ") AND ((t1.a > ?)) ORDER BY a ASC LIMIT 10", queryProvider.generateRemainingPagesQuery(10));
    }
}
