import java.io.Serializable;
//import java.util.ArrayList;
//import java.util.List;

import org.apache.spark.sql.Row;

import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

public class AnalyticsSparkFunction extends AbstractFunction1<Iterator<Row>, BoxedUnit> implements Serializable {

     private static final long serialVersionUID = 1L;
    static long MAX_RECORDS = 1000;

    @Override
    public BoxedUnit apply(Iterator<Row> iterator) {
//	List<Row> records = new ArrayList<>();
	while (iterator.hasNext()) {
//	    if (records.size() == MAX_RECORDS) {
	    // this will do stdout on spark worker
		System.out.println(iterator.next());
//		records.clear();
//	    } 
//	else {
//		Row row = iterator.next();
//		records.add(row);
//	    }
	}
//	if (!records.isEmpty()) {
////	    System.out.println(records);
//	}
	return BoxedUnit.UNIT;
    }

    // private Map<String, Object> convertRowAndSchemaToValuesMap(Row row,
    // StructType schema) {
    // String[] colNames = schema.fieldNames();
    // Map<String, Object> result = new HashMap<>();
    // for (int i = 0; i < row.length(); i++) {
    // result.put(colNames[i], row.get(i));
    // }
    // return result;
    // }

}
