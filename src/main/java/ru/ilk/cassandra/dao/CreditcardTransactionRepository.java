package ru.ilk.cassandra.dao;

//import com.datastax.driver.core.BoundStatement;
//import com.datastax.driver.core.PreparedStatement;

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

//@FunctionalInterface
//interface CqlTransactionPrepareInterface {
//
//    // абстрактный метод
//    String cqlTransactionPrepare(String db, String table);
//}
//
//@FunctionalInterface
//interface CqlTransactionBindInterface {
//    // абстрактный метод
//    BoundStatement cqlTransactionBind(PreparedStatement n);
//}

//    private static Logger logger = Logger.getLogger(CreditcardTransactionRepository.class.getName());
//
//
//    public static void main( String[] args ) {
//
//        CqlTransactionPrepareInterface ref = (db, table) -> {
//
//            return     """
//     insert into ${db}.${table} (
//       ${Enums.TransactionCassandra.cc_num},
//       ${Enums.TransactionCassandra.trans_time},
//       ${Enums.TransactionCassandra.trans_num},
//       ${Enums.TransactionCassandra.category},
//       ${Enums.TransactionCassandra.merchant},
//       ${Enums.TransactionCassandra.amt},
//       ${Enums.TransactionCassandra.merch_lat},
//       ${Enums.TransactionCassandra.merch_long},
//       ${Enums.TransactionCassandra.distance},
//       ${Enums.TransactionCassandra.age},
//       ${Enums.TransactionCassandra.is_fraud}
//     )
//     values(
//       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
//        )""";
//        };
//        // вызов метода из интерфейса
//        System.out.println("Lambda reversed = " + ref.cqlTransactionPrepare("",""));
//
//        CqlTransactionBindInterface ref1 = (prepared) -> {
//            BoundStatement bound = prepared.bind();
////            bound.setString(Enums.TransactionCassandra.cc_num, record.getAs[String](Enums.TransactionCassandra.cc_num))
////            bound.setTimestamp(Enums.TransactionCassandra.trans_time, record.getAs[Timestamp](Enums.TransactionCassandra.trans_time))
////            bound.setString(Enums.TransactionCassandra.trans_num, record.getAs[String](Enums.TransactionCassandra.trans_num))
////            bound.setString(Enums.TransactionCassandra.category, record.getAs[String](Enums.TransactionCassandra.category))
////            bound.setString(Enums.TransactionCassandra.merchant, record.getAs[String](Enums.TransactionCassandra.merchant))
////            bound.setDouble(Enums.TransactionCassandra.amt, record.getAs[Double](Enums.TransactionCassandra.amt))
////            bound.setDouble(Enums.TransactionCassandra.merch_lat, record.getAs[Double](Enums.TransactionCassandra.merch_lat))
////            bound.setDouble(Enums.TransactionCassandra.merch_long, record.getAs[Double](Enums.TransactionCassandra.merch_long))
////            bound.setDouble(Enums.TransactionCassandra.distance, record.getAs[Double](Enums.TransactionCassandra.distance))
////            bound.setInt(Enums.TransactionCassandra.age, record.getAs[Int](Enums.TransactionCassandra.age))
////            bound.setDouble(Enums.TransactionCassandra.is_fraud, record.getAs[Double](Enums.TransactionCassandra.is_fraud))
//            return bound;
//        };
//    }

}