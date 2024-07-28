package com.example.spring_batch_demo.partition;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

public class ColumnRangePartitioner implements Partitioner{

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) { //grid size = 2
        int firstRow = 1;
        int lastRow = 1000;

        int partitionSize = (lastRow - firstRow + 1)/gridSize; //partition size = 500

        int partitionId = 0;
        int start = firstRow; //1
        int end = start + partitionSize - 1; //500

        Map<String, ExecutionContext> result = new HashMap<>();

        while(start <= lastRow){
            ExecutionContext value = new ExecutionContext();
            result.put("partition"+partitionId, value);

            if(end > lastRow){
                // if there are 900 rows instead of 1000 then
                // second partition will contain rows from 501 to 900
                // not 501 to 1000
                end = lastRow;
            }

            value.putInt("minValue", start);
            value.putInt("maxValue", end);

            start += partitionSize;  //501
            end += partitionSize; //1000

            partitionId ++;
        }

        System.out.println("Partition details : \n"+result.toString());

        return result;
    }
    
    // the result will be like this
    // {
    //     "partition0": { "minValue": 1, "maxValue": 500 },
    //     "partition1": { "minValue": 501, "maxValue": 1000 }
    // }
}
