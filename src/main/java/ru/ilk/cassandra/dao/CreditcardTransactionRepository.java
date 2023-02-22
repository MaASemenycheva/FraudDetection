package ru.ilk.cassandra.dao;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import ru.ilk.creditcard.Creditcard;

import java.sql.Timestamp;

public class CreditcardTransactionRepository {
    private static Logger logger = Logger.getLogger(CreditcardTransactionRepository.class.getName());

    public static String cqlTransactionPrepare(String db,
                                               String table) {
        return "insert into "
                + db
                + "."
                + table
                + " ("
                + Creditcard.TransactionCassandra.getCc_num()
                + ", "
                + Creditcard.TransactionCassandra.getTrans_time()
                + ", "
                + Creditcard.TransactionCassandra.getTrans_num()
                + ", "
                + Creditcard.TransactionCassandra.getCategory()
                + ", "
                + Creditcard.TransactionCassandra.getMerchant()
                + ", "
                + Creditcard.TransactionCassandra.getAmt()
                + ", "
                + Creditcard.TransactionCassandra.getMerch_lat()
                + ", "
                + Creditcard.TransactionCassandra.getMerch_long()
                + ", "
                + Creditcard.TransactionCassandra.getDistance()
                + ", "
                + Creditcard.TransactionCassandra.getAge()
                + ", "
                + Creditcard.TransactionCassandra.getIs_fraud()
                +") values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    public static BoundStatement cqlTransactionBind(PreparedStatement prepared, Row record) {
        BoundStatement bound = prepared.bind();
        bound.setString(Creditcard.TransactionCassandra.getCc_num(),
                record.<String>getAs(Creditcard.TransactionCassandra.getCc_num()));
        bound.setString(Creditcard.TransactionCassandra.getTrans_time(),
                String.valueOf(record.<Timestamp>getAs(Creditcard.TransactionCassandra.getTrans_time())));
        bound.setString(Creditcard.TransactionCassandra.getTrans_num(),
                record.<String>getAs(Creditcard.TransactionCassandra.getTrans_num()));
        bound.setString(Creditcard.TransactionCassandra.getCategory(),
                record.<String>getAs(Creditcard.TransactionCassandra.getCategory()));
        bound.setString(Creditcard.TransactionCassandra.getMerchant(),
                record.<String>getAs(Creditcard.TransactionCassandra.getMerchant()));
        bound.setDouble(Creditcard.TransactionCassandra.getAmt(),
                record.<Double>getAs(Creditcard.TransactionCassandra.getAmt()));
        bound.setDouble(Creditcard.TransactionCassandra.getMerch_lat(),
                record.<Double>getAs(Creditcard.TransactionCassandra.getMerch_lat()));
        bound.setDouble(Creditcard.TransactionCassandra.getMerch_long(),
                record.<Double>getAs(Creditcard.TransactionCassandra.getMerch_long()));
        bound.setDouble(Creditcard.TransactionCassandra.getDistance(),
                record.<Double>getAs(Creditcard.TransactionCassandra.getDistance()));
        bound.setInt(Creditcard.TransactionCassandra.getAge(),
                record.<Integer>getAs(Creditcard.TransactionCassandra.getAge()));
        bound.setDouble(Creditcard.TransactionCassandra.getIs_fraud(),
                record.<Double>getAs(Creditcard.TransactionCassandra.getIs_fraud()));
        return bound;
    }

    public static String cqlTransaction(String db, String table, Row record) {
        return "insert into "
                + db
                + "."
                + table
                + " ("
                + Creditcard.TransactionCassandra.getCc_num()
                + ", "
                + Creditcard.TransactionCassandra.getTrans_time()
                + ", "
                + Creditcard.TransactionCassandra.getTrans_num()
                + ", "
                + Creditcard.TransactionCassandra.getCategory()
                + ", "
                + Creditcard.TransactionCassandra.getMerchant()
                + ", "
                + Creditcard.TransactionCassandra.getAmt()
                + ", "
                + Creditcard.TransactionCassandra.getMerch_lat()
                + ", "
                + Creditcard.TransactionCassandra.getMerch_long()
                + ", "
                + Creditcard.TransactionCassandra.getDistance()
                + ", "
                + Creditcard.TransactionCassandra.getAge()
                + ", "
                + Creditcard.TransactionCassandra.getIs_fraud()
                + ") values("
                + record.<String>getAs(Creditcard.TransactionCassandra.getCc_num())
                + ", "
                + record.<Timestamp>getAs(Creditcard.TransactionCassandra.getTrans_time())
                + ", "
                + record.<String>getAs(Creditcard.TransactionCassandra.getTrans_num())
                + ", "
                + record.<String>getAs(Creditcard.TransactionCassandra.getCategory())
                + ", "
                + record.<String>getAs(Creditcard.TransactionCassandra.getMerchant())
                + ", "
                + record.<Double>getAs(Creditcard.TransactionCassandra.getAmt())
                + ", "
                + record.<Double>getAs(Creditcard.TransactionCassandra.getMerch_lat())
                + ", "
                + record.<Double>getAs(Creditcard.TransactionCassandra.getMerch_long())
                + ", "
                + record.<Double>getAs(Creditcard.TransactionCassandra.getDistance())
                + ", "
                + record.<Double>getAs(Creditcard.TransactionCassandra.getAge())
                + ", "
                + record.<Double>getAs(Creditcard.TransactionCassandra.getIs_fraud())
                +")";
    }
}