import java.io.File;  // Import the File class
import java.io.FileNotFoundException;  // Import this class to handle errors
import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner; // Import the Scanner class to read text files
import java.util.Set;
import java.util.StringTokenizer;
import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;  // Import the IOException class to handle errors

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.*;
import java.io.*;

class db{
    static ArrayList<String> columnType = new ArrayList<>();
    static ArrayList<Integer> columnTypeLen = new ArrayList<>();
    static ArrayList<Integer> columnCard = new ArrayList<>();
    static ArrayList<ArrayList<String>> columnData = new ArrayList<ArrayList<String>>();
    public static final int file_name_padding=20;
    public static final int column_padding=10;
    public static HashSet<String> predicate_type=new HashSet<>();
    static ArrayList<String> projection_values=new ArrayList<>();
    static ArrayList<String> table_list=new ArrayList<>();
    static ArrayList<String> predicate_list=new ArrayList<>();
    static HashSet<String> predicate_list_individual=new HashSet<>();
    static Map<String, Table> tablesInInputFilesMap = new HashMap<>();
    // static Map<String, Double> ffMap = new HashMap<>();
    static ArrayList<String>check_pred=new ArrayList<>();
    static ArrayList<String>ans_ar=new ArrayList<>();
    static HashMap<String,ArrayList<String>>outmap=new HashMap<String,ArrayList<String>>();
    static List<String>predicate_text_OR=new ArrayList<>();
    static List<String>seq=new ArrayList<>();
    
    static List<Predicate> table_2_rows = new ArrayList<>();
    static Map<String,FilterFactor> ffByPredicate = new HashMap<>();
    // static List<Predicate> table_2_rows = new ArrayList<>();
    public static void main(String[] args){
        // predicate type update
        predicate_type.add("=");
        predicate_type.add("<=");
        predicate_type.add("<");
        predicate_type.add(">=");
        predicate_type.add(">");
        //
        try{
            File myObj = new File("queries.txt");
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                if (data.length()<4)
                    continue;
                switch(data.substring(0, 4)){
                    case "DROP":
                    case "drop":
                    case "Drop":
                    drop_index(data);
                    break;
                    case "LIST":
                    case "list":
                    case "List":
                    list_index(data);
                    break;
                    case "CREA":
                    case "crea":
                    case "Crea":
                    create_index( data);
                    break;
                    case "SELE":
                    case "Sele":
                    case "sele":
                    select_index(data);
                    break;
                    default:
                    break;
                }
            }
            myReader.close();
        }
        catch (Exception e) {
            display_error(e,"File Name not found") ;
          }
    }

    public static void display_error(Exception e,String s){
        System.out.println(s);
        e.printStackTrace();
    }
    
    public static void display_error(String s){
        System.out.println("Error: "+s);
    }

    public static void select_index(String data){
        
        // Todo: parse statement
        
        extractTablesFromFiles(tablesInInputFilesMap);

        HashMap<String, String> table_1_col2 = new HashMap<String, String>();
        table_1_col2.put("QBlockNo", "1");
        table_1_col2.put("AccessType", "");
        table_1_col2.put("MatchCols", "");
        table_1_col2.put("AccessName", "");
        table_1_col2.put("IndexOnly", "");
        table_1_col2.put("Prefetch", "");
        table_1_col2.put("SortC_OrderBy", "");
        table_1_col2.put("Table1Card", "");
        table_1_col2.put("Table2Card", "");
        table_1_col2.put("LeadingTable", "");


        
        for(int i=0; i<5; i++){
            Predicate predicate = new Predicate();
            table_2_rows.add(predicate);
        }

        parse_statement(data,projection_values,predicate_list_individual);

        // Todo: get display values in a list

        table_parsing(data, table_list);

        // Todo: get names to a list
        
        // todo: get predicates to a list
        // predicate_parsing(data,predicate_list,predicate_list_individual);

        if(data.contains(" OR ")){

            orparsing(data, predicate_list, check_pred, predicate_text_OR, outmap);

        }else{

            predicate_parsing( data,  predicate_list, predicate_list_individual);

        }
        // System.out.println("TESI+TING ONE THING");
        // System.out.println(predicate_list.toString());
        // System.out.println(predicate_text_OR.toString());
        // System.out.println("TESTED ONE THING");
        //Validating the input query
        // if(! validate_stmt(table_list, tablesInInputFilesMap, predicate_list, predicate_list_individual)){
        //     display_error("Couldn't validate string");
        //     System.exit(1);
        // }

        // System.out.println(table_list.toString());
        // Todo: optimize predicates

        optimize_predicates(data,projection_values,table_list,predicate_list,predicate_list_individual,tablesInInputFilesMap, table_1_col2, table_2_rows, predicate_text_OR);
        
        populateSeqOfPredicates();

        // Todo: get the final values

        // System.out.println(tablesInInputFilesMap.keySet().toString());

        list_index_select(tablesInInputFilesMap);
        
        first_table( table_1_col2) ;
        second_table( table_2_rows);

        projection_values.clear();
        table_list.clear();
        predicate_list.clear();
        predicate_list_individual.clear();
        tablesInInputFilesMap.clear();
        check_pred.clear();
        ans_ar.clear();
        outmap.clear();
        predicate_text_OR.clear();
        ffByPredicate.clear();
        seq.clear();

    }


    public static void populateSeqOfPredicates(){
        for(int i=0; i<5; i++){
            table_2_rows.get(i).seq = seq.indexOf(table_2_rows.get(i).text)+1 + "";
        }
    }

    public static void list_index_select(Map<String, Table> tablesInInputFilesMap){

        for (String keyname: tablesInInputFilesMap.keySet()) {
            System.out.println("\n\n");
            int number_of_col =5;

            // String key = name.toString();
            // String value = tablesInInputFilesMap.get(keyname).toString();
            // System.out.println(keyname + " " + value);
        
            
            String table_name=keyname;
            System.out.print(padRight("Index File Name",file_name_padding));
            for(int i=0; i<number_of_col;i++){
                System.out.print(padRight("Column "+Integer.toString(i+1), column_padding));
            }
            System.out.print(padRight("HighKey ", column_padding));
            System.out.print(padRight("Lowkey ", column_padding));
            System.out.println("");
            
            for(int i=0;i<((number_of_col+2)*column_padding)+file_name_padding;i++ ){
                System.out.print("_");
            }
            System.out.println("");

            File curDir = new File(".");
            File[] filesList = curDir.listFiles();
            for(File f : filesList){
                String name=f.getName();
                if(f.isFile() && name.substring(name.length()-3).equals("idx") && name.substring(0,table_name.length()).equals(table_name)){
                
                    // System.out.println(name.split(".idx")[0]);
                    // System.out.println(tablesInInputFilesMap.get(name.split(".idx")[0]));
                    print_row(name,tablesInInputFilesMap.get(table_name));
                }
            }

            
        }
    }

    public static void parse_statement(String data,ArrayList<String> list_values,HashSet<String> list_values_individual){
        String[] from = data.split("FROM");
        
        String[] select_part_tkr=from[0].split("SELECT");
        
        String select_part =select_part_tkr[1];
        select_part=select_part.trim();

        // System.out.println("'"+select_part+"'");

        StringTokenizer projection_string= new StringTokenizer(select_part, ",");

        while(projection_string.hasMoreTokens()){
            String temp=projection_string.nextToken().trim();
            list_values.add(temp);
            list_values_individual.add(temp);
            // System.out.println(list_values.get(list_values.size()-1));
        }
        
    }
    
    public static void table_parsing(String data,ArrayList<String> list_values){
        String[] from = data.split("FROM");
        
        String table_part=from[1];

        table_part=table_part.trim();
        // System.out.println(data);
        // System.out.println("Table parsing\n'"+table_part+"'");
        if (table_part.contains("ORDER BY")){
            String[] temp=table_part.split("ORDER BY");
            table_part=temp[0];
        }
        table_part=table_part.trim();
        // System.out.println("1'"+table_part+"'");
        if (table_part.contains("WHERE")){
            String[] temp=table_part.split("WHERE");
            table_part=temp[0];
        }
        // System.out.println("2'"+table_part+"'");
        String[] projection_string= table_part.split( ",");
        // System.out.println(projection_string.toString());

        for(String s:projection_string){
            list_values.add(s.trim());
            // System.out.println(list_values.get(list_values.size()-1));
        }
        
        // System.out.print("203:");
        // System.out.println(list_values.toString());
    }
    
    public static void predicate_parsing(String data,ArrayList<String> list_values,HashSet<String> list_values_individual ){
        String[] from = data.split("FROM");
        
        String table_part=from[1];

        table_part=table_part.trim();

        // System.out.println("'"+table_part+"'");
                
        if (table_part.contains("WHERE")){

            if (table_part.contains("ORDER BY")){
                String[] temp=table_part.split("ORDER BY");
                table_part=temp[0];
            }
            String[] temp=table_part.split("WHERE");
            
            table_part=temp[1].trim();
            // System.out.println(table_part);
            String[] pedicate_parts=table_part.split(" ");
            
            for(int i=0;i< pedicate_parts.length;i++)
            {
                // System.out.print(pedicate_parts[i]+"-");
                // System.out.print(predicate_type.contains(pedicate_parts[i]));
                String s1; 
                if (predicate_type.contains(pedicate_parts[i])){
                    list_values.add(pedicate_parts[i-1]+" " +pedicate_parts[i]+" "+ pedicate_parts[i+1]);    
                    // if(list_values.size()>0)
                    //     System.out.println(list_values.get(list_values.size()-1));
                }
                else if(pedicate_parts[i].equals("+") || pedicate_parts[i].equals("-") )
                {
                    s1=list_values.remove(list_values.size()-1);
                    s1 +=" "+ pedicate_parts[i]+" " + pedicate_parts[i+1];
                    list_values.add(s1);
                    // System.out.println(s1);
                }
                if (pedicate_parts[i].contains(".")){
                    list_values_individual.add(pedicate_parts[i]);
                }
                // System.out.println(list_values.get(list_values.size()-1));
            }
        }
    }
    
    public static void optimize_predicates(String data,ArrayList<String> projection_list_values,ArrayList<String> table_list_values,ArrayList<String> predicate_list,HashSet<String> list_values_individual ,Map<String, Table> tables,HashMap<String, String> table_1_col2, List<Predicate> table_2_rows, List<String> predicate_text_OR){
        // comment
        
        // System.out.println("Optimize");
        // System.out.println("Values till Now");
        // System.out.println(data);
        // for(String i: projection_list_values)
        //     System.out.print(i+" ");
        // System.out.println("");

        // for(String i: table_list_values)
        //     System.out.print(i+" ");
        // System.out.println("");

        // for(String i: predicate_list)
        //     System.out.print(i+" ");
        // System.out.println("");

        // for(String i: list_values_individual)
        //     System.out.print(i+" - ");
        // System.out.println("");

        // System.out.println("Values till Now END");
        ArrayList<Index> indexes=new ArrayList<>();
        ArrayList<Index> indexes_table2=new ArrayList<>();
            
        if(table_list_values.size()==1){
            // System.out.println("Single Table Access");
            if (list_index_return(table_list_values.get(0)).size()==0){
                //Check Predicates
                if (predicate_list.size()==0)
                    single_table_no_pred_no_index(table_list_values.get(0),table_1_col2,tables, table_2_rows, predicate_text_OR, predicate_list, data);
                else
                    single_table_pred_no_index(table_list_values.get(0),table_1_col2,tables, table_2_rows, predicate_text_OR, predicate_list, data);
            }
            else{
                    get_index_map(table_list_values.get(0),indexes);
                    // Check if predicate elements in index

                    //TODO: Move this to a different function DONE
                    // it will have the matching and screening stuff
                    // it will select the best index and return it
                    
                    Index_matching x=best_index( projection_list_values, predicate_list, list_values_individual ,indexes,null);

                    // System.out.println("****** best Index*****\n"+x);

                    int match_cols=x.num_of_match;
                    if (data.contains(" OR "))
                        match_cols--;
                    // check index_only

                    boolean index_only=true;
                    
                    for (String predicate:projection_list_values){
                        if (!x.index_order.contains(predicate)){
                            index_only=false;
                            break;
                        }
                    }
                    
                    //if elements in index
                    if (x.total_num!=0){
                        //TODO send values in
                        // System.out.println(data);
                        if (predicate_list.size()==0)
                            single_table_no_pred_index(table_list_values.get(0),table_1_col2,tables,index_only,match_cols, table_2_rows, predicate_text_OR, predicate_list, data, x);
                        else
                            single_table_pred_index(table_list_values.get(0),table_1_col2,tables,index_only,match_cols, table_2_rows, predicate_text_OR, predicate_list, data, x);
                    }
                    else{
                        // System.out.println(data);
                        if (predicate_list.size()==0)
                            single_table_no_pred_no_index(table_list_values.get(0),table_1_col2,tables, table_2_rows, predicate_text_OR, predicate_list, data);
                        else
                            single_table_pred_no_index(table_list_values.get(0),table_1_col2,tables, table_2_rows, predicate_text_OR, predicate_list, data);
                    }
                    
                }
        }
        else if (table_list_values.size()==2){

            // System.out.println("2 Table Access");
            get_index_map(table_list_values.get(0),indexes);
            get_index_map(table_list_values.get(1),indexes_table2);
            // no iNDEX available
            // System.out.println("**********");
            // ffByPredicate.forEach((k,v) -> {
            //     System.out.println("Predicate: "+ k + " ff1 : " + v.ff1 + " ff2 : " + v.ff2);
            // });
            if (indexes.size() == 0 && indexes_table2.size() == 0 ){
                // select leading table 
                if (predicate_list.size()==1 )
                    two_table_no_pred_no_index(table_list_values.get(0),table_list_values.get(1),table_1_col2,tables, table_2_rows, predicate_text_OR, predicate_list, data);
                else
                    two_table_pred_no_index(table_list_values.get(0),table_list_values.get(1),table_1_col2,tables, table_2_rows, predicate_text_OR, predicate_list, data);
            }
            else{
                String join_predicate=null;

                // if(indexes.size()>0 && indexes_table2.size()>0 ){
                for(String s: predicate_list){
                    if(s.contains(table_list_values.get(0)) && s.contains(table_list_values.get(1))){
                        join_predicate=s;
                        break;
                    }
                }
                

                Index_matching table1_index=indexes.size()>0?best_index( projection_list_values, predicate_list, list_values_individual ,indexes,join_predicate):null;
                Index_matching table2_index=indexes_table2.size()>0?best_index( projection_list_values, predicate_list, list_values_individual ,indexes_table2,join_predicate):null;

                

                if ((table1_index==null && table2_index==null)||((table1_index!=null && table1_index.total_num ==0) && (table2_index!=null && table2_index.total_num ==0))){
                    // no Index Situation
                    if (predicate_list.size()==1){
                        two_table_no_pred_no_index(table_list_values.get(0),table_list_values.get(1),table_1_col2,tables, table_2_rows, predicate_text_OR, predicate_list, data);
                    }else{
                        two_table_pred_no_index(table_list_values.get(0),table_list_values.get(1),table_1_col2,tables, table_2_rows, predicate_text_OR, predicate_list, data);
                    }
                }
                else{
                    Index_matching outer=null;
                    Index_matching inner=null;
                    // System.out.println("**************hfajhkjadhfhdafk***************************");
                    // System.out.println(table1_index);
                    // System.out.println(table2_index);
                    // System.out.println(table2_index==null);
                    // System.out.println(table1_index==null);
                    populateTable2TextColumn(table_2_rows, predicate_text_OR, predicate_list, data);

                    populateTable2Cardinality(table_2_rows, predicate_text_OR, predicate_list, data);
            
                    populateTable2Type(table_2_rows, predicate_text_OR, predicate_list, data);
            
                    populateTable2FilterFactor(table_2_rows, predicate_text_OR, predicate_list, data);
                    if(table1_index==null){
                        inner=table2_index;
                        outer=table1_index;
                    }
                    else if(table2_index==null){
                        // System.out.println("EXECUTING");
                        outer=table2_index;
                        inner=table1_index;
                    }
                    else{ 
                        //TODO if eq match cols
                        // greater ff will be 
                        // System.out.println("Shit");
                        if(table1_index.num_of_match>table2_index.num_of_match){
                            outer=table2_index;
                            inner=table1_index;
                        }
                        else if (table1_index.num_of_match<table2_index.num_of_match){
                            inner=table2_index;
                            outer=table1_index;
                        }
                        else{

                            Table temp = tables.get(table1_index.index_name.contains(table_list_values.get(0))?table_list_values.get(0):table_list_values.get(1));
                            Table temp2 = tables.get(table1_index.index_name.contains(table_list_values.get(0))?table_list_values.get(1):table_list_values.get(0));
                            // inner=table1_index.filter_factor.get(0)<table2_index.filter_factor.get(0)? table1_index:table2_index;
                            // outer=table1_index.filter_factor.get(0)<table2_index.filter_factor.get(0)? table2_index:table1_index;
                            Table temp_inner=get_outer_table(temp, temp2);
                            inner=table1_index.index_name.contains(temp_inner.tableName)?table1_index:table2_index;
                            outer=table1_index.index_name.contains(temp_inner.tableName)?table2_index:table1_index;
                        }
                    }

                    String leading=table_list_values.get(0);
                    String inner_table=table_list_values.get(1);

                    int match_cols=inner.num_of_match;
                    // check index_onlysysout
                    // System.out.println("*************************");
                    // System.out.println(data.contains(" OR "));
                    if (data.contains(" OR "))
                        match_cols--;

                    boolean index_only=true;
                    
                    for (String predicate:projection_list_values){
                        if (!inner.index_order.contains(predicate)){
                            index_only=false;
                            break;
                        }
                    }
                    if (predicate_list.size()==1){
                        two_table_no_pred_index(leading,inner_table,table_1_col2,tables,index_only,match_cols, table_2_rows, predicate_text_OR, predicate_list, data,outer,inner);
                    }
                    else{
                        two_table_pred_index(leading,inner_table,table_1_col2,tables,index_only,match_cols, table_2_rows, predicate_text_OR, predicate_list, data,outer,inner);
                    }
                    // System.out.println(seq.toString());
                }
                // System.out.println(seq.toString());
            }
        }
        else{
            display_error( "Number of table not valid");
            System.err.println(table_list_values.size());
            System.err.println(table_list_values.toString());
        }
    }

    public static void print_arlist(ArrayList<String> arlist){
        for (String s : arlist){
            System.out.print(s+" - ");
        }
        System.out.println("");
    }
    
    public static void print_arlist_int(ArrayList<Integer> arlist){
        for (int s : arlist){
            System.out.print(s+" - ");
        }
        System.out.println("");
    }
    
    public static void print_arlist_double(ArrayList<Double> arlist){
        for (double s : arlist){
            System.out.print(s+" - ");
        }
        System.out.println("");
    }
    
    public static boolean check_sort_required(Index_matching best_index,String data){
        // System.out.println(data);
        if(!data.contains("ORDER BY"))
            return false;
        if(best_index==null){
            return true;
        }
        String[] order_by_data = data.split("ORDER BY")[1].trim().split(" , ");
        // for (String s:order_by_data)
        //     System.out.println(s);
        // System.out.println(best_index.index_order.toString());
        if (order_by_data.length>best_index.index_order.size())
            return true;
        for(int i=0;i<order_by_data.length;i++){
            // System.out.println(order_by_data[i].equals(best_index.index_order.get(i)));
            // System.out.println(i);
            String s=order_by_data[i].trim();
            if(order_by_data[i].contains(" "))
                s=order_by_data[i].split(" ")[0];
            if (!s.equals(best_index.index_order.get(i)))
                return true;
        }
        
        return false;
    }

    public static Index_matching best_index(ArrayList<String> projection_list_values,ArrayList<String> predicate_list,HashSet<String> list_values_individual ,List<Index> indexs, String join_predicate){
        // no In list predicates checked
        
        ArrayList<Index_matching> matching_screening=new ArrayList<>();

        // System.out.println("TESTING BEST INDEX");

        int max_matching=0;
        
        for (Index i : indexs){
            //for each index
            String mark="matching";
            ArrayList<String> matching_list=new ArrayList<>();
            // System.out.println(i.order.toString());
            boolean on_join=false;
            for (String item:i.order){
                // iterating index order
                boolean change=false;
                // System.out.println(predicate_list.toString());
                
                for (String predicate:predicate_list){
                    // checking if index order aligns with predicate 
                    String[] parts=predicate.split(" ");
                    String first_part=parts[0];
                    String operator_part=parts[1];
                    // String operator_part=parts[1];
                    // System.out.println(item+"  " +first_part);
                    
                    if(item.equals(first_part)){
                        change=true;
                        if (mark.equals("matching")){
                            
                            if (operator_part.contains("<") || operator_part.contains(">") ){
                                mark="screening";
                                matching_list.add("matching");
                            }
                            else{
                                matching_list.add("matching");
                            }
                        }
                        else{
                            matching_list.add("screening");
                        }
                    }
                }
                //if no match in the pred list for index:
                if(!change){
                    mark="screening";
                }

            }
            int matching_number=Collections.frequency(matching_list, "matching");
                
            if(join_predicate!=null && join_predicate.contains(i.order.get(0)) && matching_number>=1){
                on_join=true;
            }
            if(join_predicate==null){
            
                if (matching_number<max_matching){
                    continue;
                }
                if (matching_number>max_matching){
                    max_matching=matching_number;
                    matching_screening.clear();
                }
                int size=matching_list.size();
                for(String s: projection_list_values){
                    for (String item:i.order){
                        if(s.equals(item)){
                            size++;
                        }
                    }
                }
                matching_screening.add(new Index_matching(i.index_name, matching_list,matching_number,size,i.filter_factor,i.order));
            }
            else{
                if(on_join){
                    if (matching_number<max_matching){
                        continue;
                    }
                    if (matching_number>max_matching){
                        max_matching=matching_number;
                        matching_screening.clear();
                    }
                    int size=matching_list.size();
                    for(String s: projection_list_values){
                        for (String item:i.order){
                            if(s.equals(item)){
                                size++;
                            }
                        }
                    }
                    matching_screening.add(new Index_matching(i.index_name, matching_list,matching_number,size,i.filter_factor,i.order));
                }
            }
        }

        // for (Index_matching temp:matching_screening){
        //     // System.out.print(temp.index_name+" -- ");
        //     // System.out.println(temp.order.toString());
        //     print_arlist_double(temp.filter_factor);
        // }
        // ArrayList<String> best_list=matching_screening.get(0);
        if(matching_screening.size()==0)
            return null;
        Collections.sort(matching_screening, new Comparator<Index_matching>() {

            public int compare(Index_matching o1, Index_matching o2) {
                // compare two instance of `Score` and return `int` as result.
                return Integer.compare(o2.total_num,o1.total_num);
            }
        });
        
        Collections.sort(matching_screening, new Comparator<Index_matching>() {

            public int compare(Index_matching o1, Index_matching o2) {
                // compare two instance of `Score` and return `int` as result.
                return Double.compare(o1.filter_factor.get(0),o2.filter_factor.get(0));
            }
        });
        
        // for (Index_matching temp:matching_screening){
        //     System.out.print(temp.index_name+" -- ");
        //     print_arlist(temp.order);
        //     // print_arlist_int(temp.filter_factor);
        // }
        // System.out.println("TESTING BEST INDEX END");
        
        return matching_screening.get(0);
        
    }
    
    public static boolean validate_stmt(ArrayList<String> table_list, Map<String,Table> tablesInInputFilesMap, ArrayList<String> predicate_list, HashSet<String> predicate_list_individual){
        //Check the tables and columns in query are present in the tables in files and given columns
        Set<String> inputFileTables = tablesInInputFilesMap.keySet();
        Set<String> checkPredicates = new HashSet<>();
        
        for(String predValue : predicate_list_individual){
            if(!checkTableNameAndColLength(predValue, inputFileTables, tablesInInputFilesMap)){
                display_error( "Table name and col length does not match with the number of columns or tables present in the database");
                return false;
            }
        }

        //CHeck if same predicate is applied twice
        for(String check : predicate_list){
            if(check.contains(">") || check.contains("<")){
                check = check.replaceAll(">", "=").replaceAll("<", "=");
            }
            // String predValue = check.toUpperCase();
            String predValue = check;
            
            checkPredicates.add(predValue);
            String parts[] = predValue.split("=");
            parts[0] = parts[0].trim();
            parts[1] = parts[1].trim();
            if(parts[1].contains(".")){
                if(!checkTableNameAndColLength(parts[1], inputFileTables, tablesInInputFilesMap)){
                    display_error( "Table name and col length Part 1");
                    return false;
                }
            }else{
                String inputQueryDataType = "";
                String old = new String(parts[1]);
                String charRemovedValue = parts[1].replaceAll("[A-Z]", "");
                if(charRemovedValue.length() == 0){
                    inputQueryDataType = "C";
                }else if(old.length() > charRemovedValue.length()){
                    display_error( "DataType mismatch in the input query");
                    return false;
                }else {
                    inputQueryDataType = "I";
                }
                String[] part0SubParts = parts[0].split("\\.");
                
                int colId = Integer.parseInt(part0SubParts[1].replaceAll("[A-Z]", ""))-1;
                if(!tablesInInputFilesMap.get(part0SubParts[0]).columns.get(colId).dataType.equals(inputQueryDataType)){
                    display_error( "DataType of the columns don't match with the given input");
                    return false;
                }
            }
        }
        // System.out.println(predicate_list.toString());
        // System.out.println(checkPredicates.toString());
        if(checkPredicates.size() != predicate_list.size()){
            display_error( "Predicates are being repeated");
            return false;
        }

        // System.out.println("PREDICATES:::");

        // for(String input : predicate_list){
        //     System.out.println(input);
        // }

        // System.out.println("PREDICATES END:::");


        return true;
    } 

    public static boolean checkTableNameAndColLength(String predValue, Set<String> inputFileTables, Map<String,Table> tablesInInputFilesMap){
        String[] input = predValue.split("\\.");
        String tableName = input[0];
        String colName = input[1];
        if(!inputFileTables.contains(tableName)){
            return false;
        }
        int colNo = Integer.parseInt(colName.replaceAll("[A-Z]", ""));
        if(colNo > tablesInInputFilesMap.get(tableName).columns.size()){
            return false;
        }
        return true;
    }

    public static void single_table_no_pred_no_index(String Table_name,Map<String, String> table_1_col2,Map<String, Table> tables, List<Predicate> table_2_rows, List<String> prediciate_text_OR, List<String> prediciate_list, String data){
        table_1_col2.replace("AccessType", "R");
        table_1_col2.replace("MatchCols", "0");
        table_1_col2.replace("AccessName", "");
        table_1_col2.replace("IndexOnly", "N");
        table_1_col2.replace("Prefetch", "S");
        table_1_col2.replace("SortC_OrderBy", check_sort_required(null, data)?"Y":"N");// TODO
        Table tab= tables.get(Table_name);
        table_1_col2.replace("Table1Card", ""+tab.numberOfRows);
        table_1_col2.replace("Table2Card", "");
        table_1_col2.replace("LeadingTable", "");

        populateTable2TextColumn(table_2_rows, prediciate_text_OR, prediciate_list, data);
        
        populateTable2Cardinality(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2Type(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2FilterFactor(table_2_rows, prediciate_text_OR, prediciate_list, data);
        get_outer_table(tab, null);
    }
    
    public static void single_table_pred_no_index(String table_name,HashMap<String, String> table_1_col2, Map<String, Table> tables, List<Predicate> table_2_rows, List<String> prediciate_text_OR, List<String> prediciate_list, String data){
        table_1_col2.replace("AccessType", "R");
        table_1_col2.replace("MatchCols", "0");
        table_1_col2.replace("AccessName", "");
        table_1_col2.replace("IndexOnly", "N");
        table_1_col2.replace("Prefetch", "S");
        table_1_col2.replace("SortC_OrderBy", check_sort_required(null, data)?"Y":"N");// TODO
        // table_1_col2.
        // int tcard=0;
        Table tab= tables.get(table_name);
        table_1_col2.replace("Table1Card", ""+tab.numberOfRows);
        table_1_col2.replace("Table2Card", "");
        table_1_col2.replace("LeadingTable", "");

        populateTable2TextColumn(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2Cardinality(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2Type(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2FilterFactor(table_2_rows, prediciate_text_OR, prediciate_list, data);
        get_outer_table(tab, null);
    }
    
    public static void single_table_no_pred_index(String table_name,HashMap<String, String> table_1_col2, Map<String, Table> tables,boolean index_only,int match_cols, List<Predicate> table_2_rows, List<String> prediciate_text_OR, List<String> prediciate_list, String data,Index_matching best_index){
        // System.out.println("single_table_no_pred_index");
        table_1_col2.replace("AccessType", "I");
        table_1_col2.replace("MatchCols", ""+match_cols);
        table_1_col2.replace("AccessName", best_index.index_name.split(".idx")[0]);
        table_1_col2.replace("IndexOnly", index_only?"Y":"N");
        table_1_col2.replace("Prefetch", "");
        table_1_col2.replace("SortC_OrderBy", check_sort_required(best_index, data)?"Y":"N");// TODO
        // table_1_col2.
        // int tcard=0;
        Table tab= tables.get(table_name);
        table_1_col2.replace("Table1Card", ""+tab.numberOfRows);
        table_1_col2.replace("Table2Card", "");
        table_1_col2.replace("LeadingTable", "");

        populateTable2TextColumn(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2Cardinality(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2Type(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2FilterFactor(table_2_rows, prediciate_text_OR, prediciate_list, data);
        get_outer_table(tab, null);
    }
    
    public static void single_table_pred_index(String table_name,HashMap<String, String> table_1_col2, Map<String, Table> tables,boolean index_only,int match_cols, List<Predicate> table_2_rows, List<String> prediciate_text_OR, List<String> prediciate_list, String data,Index_matching best_index){
        table_1_col2.replace("AccessType", "I");
        table_1_col2.replace("MatchCols", ""+match_cols);
        table_1_col2.replace("AccessName", best_index.index_name.split(".idx")[0]);
        table_1_col2.replace("IndexOnly", index_only?"Y":"N");
        table_1_col2.replace("Prefetch", "");
        table_1_col2.replace("SortC_OrderBy", check_sort_required(best_index, data)?"Y":"N");// TODO
        // table_1_col2.
        // int tcard=0;
        Table tab= tables.get(table_name);
        table_1_col2.replace("Table1Card", ""+tab.numberOfRows);
        table_1_col2.replace("Table2Card", "");
        table_1_col2.replace("LeadingTable", "");

        populateTable2TextColumn(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2Cardinality(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2Type(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2FilterFactor(table_2_rows, prediciate_text_OR, prediciate_list, data);
        get_outer_table(tab, null,best_index,null);
    }
    
    // Two table Stuff
    public static void two_table_no_pred_no_index(String leading_table_name,String inner_table,Map<String, String> table_1_col2,Map<String, Table> tables, List<Predicate> table_2_rows, List<String> prediciate_text_OR, List<String> prediciate_list, String data){
        populateTable2TextColumn(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2Cardinality(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2Type(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2FilterFactor(table_2_rows, prediciate_text_OR, prediciate_list, data);

        table_1_col2.replace("AccessType", "R");
        table_1_col2.replace("MatchCols", "0");
        table_1_col2.replace("AccessName", "");
        table_1_col2.replace("IndexOnly", "N");
        table_1_col2.replace("Prefetch", "S");
        table_1_col2.replace("SortC_OrderBy", check_sort_required(null, data)?"Y":"N");// TODO
        // table_1_col2.
        // int tcard=0;
        Table table1= tables.get(leading_table_name);
        Table table2= tables.get(inner_table);
        table_1_col2.replace("Table1Card", ""+table1.numberOfRows);
        table_1_col2.replace("Table2Card", ""+table2.numberOfRows);

        String outer_table=table1.numberOfRows>table2.numberOfRows?table1.tableName:table2.tableName;
        
        table_1_col2.replace("LeadingTable", outer_table);
        get_outer_table(table1, table2);

    }
    
    public static void two_table_pred_no_index(String leading_table_name,String inner_table,HashMap<String, String> table_1_col2, Map<String, Table> tables, List<Predicate> table_2_rows, List<String> prediciate_text_OR, List<String> prediciate_list, String data){
        populateTable2TextColumn(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2Cardinality(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2Type(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2FilterFactor(table_2_rows, prediciate_text_OR, prediciate_list, data);

        table_1_col2.replace("AccessType", "R");
        table_1_col2.replace("MatchCols", "0");
        table_1_col2.replace("AccessName", "");
        table_1_col2.replace("IndexOnly", "N");
        table_1_col2.replace("Prefetch", "S");
        table_1_col2.replace("SortC_OrderBy", check_sort_required(null, data)?"Y":"N");// TODO
        // table_1_col2.
        // int tcard=0;
        Table table1= tables.get(leading_table_name);
        Table table2= tables.get(inner_table);
        table_1_col2.replace("Table1Card", ""+table1.numberOfRows);
        table_1_col2.replace("Table2Card", ""+table2.numberOfRows);

        Table outer_table=get_outer_table(table1, table2);

        String inner_tab=outer_table.tableName.contains(leading_table_name)?inner_table:leading_table_name;
        
        table_1_col2.replace("LeadingTable", inner_tab);

    }
    
    public static void two_table_no_pred_index(String leading_table_name,String inner_table,HashMap<String, String> table_1_col2, Map<String, Table> tables,boolean index_only,int match_cols, List<Predicate> table_2_rows, List<String> prediciate_text_OR, List<String> prediciate_list, String data, Index_matching outer, Index_matching inner){
        
        populateTable2TextColumn(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2Cardinality(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2Type(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2FilterFactor(table_2_rows, prediciate_text_OR, prediciate_list, data);

        table_1_col2.replace("AccessType", "I");
        table_1_col2.replace("MatchCols", ""+match_cols);
        table_1_col2.replace("AccessName", inner.index_name.split(".idx")[0]);
        table_1_col2.replace("IndexOnly", index_only?"Y":"N");
        table_1_col2.replace("Prefetch", (outer==null)?"S":""); //TODO
        table_1_col2.replace("SortC_OrderBy", check_sort_required(outer, data) || check_sort_required(inner, data) ?"Y":"N");// TODO
        // table_1_col2.
        // int tcard=0;
        Table table1= tables.get(leading_table_name);
        Table table2= tables.get(inner_table);
        table_1_col2.replace("Table1Card", ""+table1.numberOfRows);
        table_1_col2.replace("Table2Card", ""+table2.numberOfRows);

        Table outer_table=get_outer_table(table1, table2);// to get sequence
        // String outer_table=get_outer_table(table1, table2);

        String leading_table= inner.index_name.startsWith(leading_table_name)?inner_table:leading_table_name;
        
        table_1_col2.replace("LeadingTable", outer_table.tableName);

    }
    
    public static void two_table_pred_index(String leading_table_name,String inner_table,HashMap<String, String> table_1_col2, Map<String, Table> tables,boolean index_only,int match_cols, List<Predicate> table_2_rows, List<String> prediciate_text_OR, List<String> prediciate_list, String data,Index_matching outer , Index_matching inner){
        populateTable2TextColumn(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2Cardinality(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2Type(table_2_rows, prediciate_text_OR, prediciate_list, data);

        populateTable2FilterFactor(table_2_rows, prediciate_text_OR, prediciate_list, data);
        table_1_col2.replace("AccessType", "I");
        table_1_col2.replace("MatchCols", ""+match_cols);
        table_1_col2.replace("AccessName", inner.index_name.split(".idx")[0]);
        table_1_col2.replace("IndexOnly", index_only?"Y":"N");
        table_1_col2.replace("Prefetch", (outer==null)?"S":"");
        table_1_col2.replace("SortC_OrderBy", check_sort_required(outer, data) || check_sort_required(inner, data)?"Y":"N");// TODO
        // table_1_col2.
        // int tcard=0;
        Table table1= tables.get(leading_table_name);
        Table table2= tables.get(inner_table);
        table_1_col2.replace("Table1Card", ""+table1.numberOfRows);
        table_1_col2.replace("Table2Card", ""+table2.numberOfRows);

        Table outer_table=get_outer_table(table1, table2,outer,inner);// to get sequence
        // String outer_table=get_outer_table(table1, table2);
        
        // String leading_table= outer.index_name.startsWith(leading_table_name)?leading_table_name:inner_table;
        // String leading_table= inner.index_name.startsWith(leading_table_name)?inner_table:leading_table_name;
        
        table_1_col2.replace("LeadingTable", outer_table.tableName);

    }
    
    public static Table get_outer_table(Table t1, Table t2){
        List<String> temp = predicate_text_OR.size()>0 ? predicate_text_OR : predicate_list;

        ArrayList<String> table1=new ArrayList<>();
        ArrayList<String> table2=new ArrayList<>();
        ArrayList<String> join_table=new ArrayList<>();

        if(t2!=null){
            for(String s : temp){
                if (s.contains(t1.tableName) && s.contains(t2.tableName))
                    join_table.add(s);
                else if (s.contains(t1.tableName) )
                    table1.add(s);
                else if ( s.contains(t2.tableName))
                    table2.add(s);
            }
        }
        else{
            for(String s : temp){
                if (s.contains(t1.tableName) )
                    table1.add(s);
            }
        }

        // Sel * from T1 , T2 WHERE T1.1 = 10 AND T1.2 = 20 AND T1.1=T2.1
        //         1000 R   200R      0.5 FF      0.3 FF

        // TODO
        // get outer table
        // get sequence of predicates

        double t1_ff=1.0;
        for (String s : table1){
            t1_ff *= ffByPredicate.get(s).ff1;
        }
        double t2_ff=1.0;
        for (String s : table2){
            // System.out.println(s);
            t2_ff *= ffByPredicate.get(s).ff1;
        }
        ArrayList<String> outer;
        ArrayList<String> inner;
        Table outer_name;
        Table inner_name;

        if(t2!=null){
            // System.out.println(t1_ff * t1.numberOfRows+"  ___  "+t2_ff * t2.numberOfRows);
            // System.out.println(t1_ff +"-- "+ t1.numberOfRows+"__"+t2_ff+"--"+t2.numberOfRows);
            outer= t1_ff * t1.numberOfRows>t2_ff * t2.numberOfRows? table2:table1;
            inner= t1_ff * t1.numberOfRows>t2_ff * t2.numberOfRows? table1:table2;
        
            outer_name=t1_ff * t1.numberOfRows>t2_ff * t2.numberOfRows? t2:t1;
            inner_name=t1_ff * t1.numberOfRows>t2_ff * t2.numberOfRows? t1:t2;
        }
        else{
            outer=table1;
            inner=table2;
            outer_name=t1;
            inner_name=null;
        }

        Collections.sort(outer, new Comparator<String>() {

            public int compare(String o1, String o2) {
                // compare two instance of `Score` and return `int` as result.

                return Double.compare(ffByPredicate.get(o1).ff1,ffByPredicate.get(o2).ff1);
            }
        });
        Collections.sort(inner, new Comparator<String>() {

            public int compare(String o1, String o2) {
                // compare two instance of `Score` and return `int` as result.

                return Double.compare(ffByPredicate.get(o1).ff1,ffByPredicate.get(o2).ff1);
            }
        });
        
        seq.addAll(outer);
        seq.addAll(join_table);
        seq.addAll(inner);
        // System.out.println("^^^^^^^^^^^^^^^^");
        // System.out.println(seq.toString());
        return outer_name;
    }
    public static Table get_outer_table(Table t1, Table t2,Index_matching outer_index , Index_matching inner_Index){
        List<String> temp = predicate_text_OR.size()>0 ? predicate_text_OR : predicate_list;

        ArrayList<String> table1=new ArrayList<>();
        ArrayList<String> table2=new ArrayList<>();
        ArrayList<String> join_table=new ArrayList<>();

        if(t2!=null){
            for(String s : temp){
                if (s.contains(t1.tableName) && s.contains(t2.tableName))
                    join_table.add(s);
                else if (s.contains(t1.tableName) )
                    table1.add(s);
                else if ( s.contains(t2.tableName))
                    table2.add(s);
            }
        }
        else{
            for(String s : temp){
                if (s.contains(t1.tableName) )
                    table1.add(s);
            }
        }

        // Sel * from T1 , T2 WHERE T1.1 = 10 AND T1.2 = 20 AND T1.1=T2.1
        //         1000 R   200R      0.5 FF      0.3 FF

        // TODO
        // get outer table
        // get sequence of predicates

        double t1_ff=1.0;
        for (String s : table1){
            t1_ff *= ffByPredicate.get(s).ff1;
        }
        double t2_ff=1.0;
        for (String s : table2){
            // System.out.println(s);
            t2_ff *= ffByPredicate.get(s).ff1;
        }
        ArrayList<String> outer;
        ArrayList<String> inner;
        Table outer_name;
        Table inner_name;

        if(t2!=null){
            // System.out.println(t1_ff * t1.numberOfRows+"  ___  "+t2_ff * t2.numberOfRows);
            // System.out.println(t1_ff +"-- "+ t1.numberOfRows+"__"+t2_ff+"--"+t2.numberOfRows);
            outer= t1_ff * t1.numberOfRows>t2_ff * t2.numberOfRows? table2:table1;
            inner= t1_ff * t1.numberOfRows>t2_ff * t2.numberOfRows? table1:table2;
        
            outer_name=t1_ff * t1.numberOfRows>t2_ff * t2.numberOfRows? t2:t1;
            inner_name=t1_ff * t1.numberOfRows>t2_ff * t2.numberOfRows? t1:t2;
        }
        else{
            outer=table1;
            inner=table2;
            outer_name=t1;
            inner_name=null;
        }

        Collections.sort(outer, new Comparator<String>() {

            public int compare(String o1, String o2) {
                // compare two instance of `Score` and return `int` as result.

                return Double.compare(ffByPredicate.get(o1).ff1,ffByPredicate.get(o2).ff1);
            }
        });
        Collections.sort(inner, new Comparator<String>() {

            public int compare(String o1, String o2) {
                // compare two instance of `Score` and return `int` as result.

                return Double.compare(ffByPredicate.get(o1).ff1,ffByPredicate.get(o2).ff1);
            }
        });

        if(outer_index!=null){
            int swap=0;
            for(String index_ele : outer_index.index_order){
                // System.out.print("   "+outer_index.index_order.toString());
                for(int i=0;i<outer.size();i++){
                    // System.out.print("\n   --"+outer.get(i));
                    if(outer.get(i).contains(index_ele)){
                        Collections.swap(outer, i, swap);
                        swap++;
                        break;
                    }
                }
            }
        }
        if(inner_Index!=null){
            // System.out.println("   ++"+inner_Index.index_order.toString());
            int swap=0;
            for(String index_ele : inner_Index.index_order){
                // System.out.println("---   ++"+index_ele);
                for(int i=0;i<inner.size();i++){
                    // System.out.println("\n   --"+inner.get(i)+inner.get(i).contains(index_ele));
                    if(inner.get(i).contains(index_ele)){
                        Collections.swap(inner, i, swap);
                        swap++;
                        break;
                    }
                }
            }
        }

        seq.addAll(outer);
        seq.addAll(join_table);
        seq.addAll(inner);
        // System.out.println("^^^^^^^^^^^^^^^^");
        // System.out.println(seq.toString());
        return outer_name;
    }

    public static HashMap<String, ArrayList<String>> get_index_map(String table_name,ArrayList<Index> index){
        ArrayList<String> indexes=list_index_return(table_name);
        HashMap<String, ArrayList<String>> index_map = new HashMap<>();
        HashMap<String,Double> filfact=new HashMap<String,Double>();
        try {
            File myObj = new File(table_name+".tab");
            Scanner myReader = new Scanner(myObj);
            myReader.nextLine();
            String [] data = myReader.nextLine().split(" ");
            for (int i=0;i<data.length;i++){
                double ele=Integer.parseInt(data[i])+0.0;
                if (ele==-1)
                    ele=25;
                // System.out.println(table_name+"."+(i+1)+"  -  " +1/ele);
                filfact.put(table_name+"."+(i+1), 1/ele);
            }
            myReader.close();
        }
        catch (Exception e){
            display_error(e, "Table FIle Not Found");
        }
        for (String s: indexes){
            String[] data={};
            ArrayList<String> temp_index_val=new ArrayList<>();
            ArrayList<Double> temp_Filter_fact_val=new ArrayList<>();
            try {
                File myObj = new File(s);
                Scanner myReader = new Scanner(myObj);
                data = myReader.nextLine().split(" ");
                
                for (String temp : data){
                    String col_val=table_name+"."+temp.substring(0, temp.length()-1);
                    temp_index_val.add(col_val);
                    temp_Filter_fact_val.add(filfact.get(col_val));
                    // System.out.print(table_name+"."+temp.substring(0, temp.length()-1)+" - ");
                }
                int col_card=Integer.parseInt(myReader.nextLine());
                myReader.close();
                index.add(new Index(s, temp_index_val, col_card,temp_Filter_fact_val));
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            // System.out.println(s);
            index_map.put(s, temp_index_val);
        }
        return index_map;
    }

    public static void populateTable2TextColumn(List<Predicate> table_2_rows, List<String> predicate_text_OR, List<String> predicate_list, String data ){
        if(data.contains(" OR ")){
            
            for(int j=0;j<predicate_text_OR.size();j++){
                // System.out.println("ENTERED IN TYPE 1");
                
                table_2_rows.get(j).text = predicate_text_OR.get(j);
            }
        }else{
            for(int j=0;j<predicate_list.size();j++){
                table_2_rows.get(j).text = predicate_list.get(j);
            }
        }
    }

    public static void populateTable2FilterFactor(List<Predicate> table_2_rows, List<String> predicate_text_OR, List<String> predicate_list, String data ){
        if(data.contains(" OR ")){
            String ans  = getFilterFactor(predicate_list.get(0).split("=")[0], "E","")*predicate_list.size() + "";
            table_2_rows.get(0).ff1 = ans.length() > 4 ? ans.substring(0, 4) : ans;
            ffByPredicate.put(predicate_text_OR.get(0), new FilterFactor(Double.parseDouble(table_2_rows.get(0).ff1),0));
        }else{
            for(int i=0; i<predicate_list.size(); i++){
                String predValue = predicate_list.get(i);
                
                if(predValue.contains(">") ){
                    String ans  = getFilterFactor(predValue.split(">")[0], "RH", predValue.split(">")[1]) + "";
                    table_2_rows.get(i).ff1 = ans.length() > 4 ? ans.substring(0, 4) : ans;
                    ffByPredicate.put(predicate_list.get(i), new FilterFactor(Double.parseDouble(table_2_rows.get(i).ff1),0));
                }
                else if(predValue.contains("<")){
                    String ans = getFilterFactor(predValue.split("<")[0], "RL", predValue.split("<")[1]) + "";
                    table_2_rows.get(i).ff1 = ans.length() > 4 ? ans.substring(0, 4) : ans;
                    ffByPredicate.put(predicate_list.get(i), new FilterFactor(Double.parseDouble(table_2_rows.get(i).ff1),0));
                }else if(predValue.split("=")[1].contains(".")){
                    
                    String parts[] = predValue.split("=");

                    String ans = getFilterFactor(parts[0].trim(), "E","") + "";
                    table_2_rows.get(i).ff1 = ans.length() > 4 ? ans.substring(0, 4) : ans;
                    String ans2 = getFilterFactor(parts[1].trim(), "E","") + "";
                    table_2_rows.get(i).ff2 = ans2.length() > 4 ? ans2.substring(0,4) : ans2;
                    // System.out.println("FF2 SETTING AS  :" + table_2_rows.get(i).ff2);
                    ffByPredicate.put(predicate_list.get(i), new FilterFactor(Double.parseDouble(table_2_rows.get(i).ff1),Double.parseDouble(table_2_rows.get(i).ff1)));
                }else{
                    // System.out.println("CALLED JOIN + " + predValue);
                    String ans = getFilterFactor(predValue.split("=")[0].trim(), "E","") + "";
                    table_2_rows.get(i).ff1 = ans.length() > 4 ? ans.substring(0, 4) : ans;
                    ffByPredicate.put(predicate_list.get(i), new FilterFactor(Double.parseDouble(table_2_rows.get(i).ff1),0));
                }
            
            }
        }
    }   

    public static double getFilterFactor(String predValue, String predType, String no){
        // System.out.println("PRED TYPE:" + predType);
        String parts[] = predValue.split("\\.");
        String tableName = parts[0].trim();
        if(tablesInInputFilesMap.get(tableName).numberOfRows == 0){
            return -1;
        }
        int colId = Integer.parseInt(parts[1].trim())-1;
        double rangeRhsValue = 0;
        double ff = 0;
        switch(predType){
            case "RH": {
                String dataType = tablesInInputFilesMap.get(tableName).columns.get(colId).dataType;
                if(dataType.equals("I")){
                    rangeRhsValue = Double.parseDouble(no.trim());
                    double low2Key = Double.parseDouble(tablesInInputFilesMap.get(tableName).columns.get(colId).low2Key);
                    double high2Key = Double.parseDouble(tablesInInputFilesMap.get(tableName).columns.get(colId).high2Key);
                    // System.out.println("1 highL :" + high2Key + " low: "+ low2Key + " no : " + rangeRhsValue);
                    double num = Math.abs(high2Key - rangeRhsValue);
                    double den = Math.abs(high2Key - low2Key);
                    ff = num/den;
                }else{
                    display_error("Range predicate cannot have character comparisons");
                }
                break;
            }
            case "RL": {
                String dataType = tablesInInputFilesMap.get(tableName).columns.get(colId).dataType;
                if(dataType.equals("I")){
                    rangeRhsValue = Double.parseDouble(no.trim());
                    double low2Key = Double.parseDouble(tablesInInputFilesMap.get(tableName).columns.get(colId).low2Key);
                    double high2Key = Double.parseDouble(tablesInInputFilesMap.get(tableName).columns.get(colId).high2Key);
                    // System.out.println("2 highL :" + high2Key + " low: "+ low2Key + " no : " + rangeRhsValue);
                    double num = Math.abs(rangeRhsValue - low2Key);
                    double den = Math.abs(high2Key - low2Key);
                    ff = num/den;
                }else{
                    display_error("Range predicate cannot have character comparisons");
                }
                break;
            }
            case "E" : {
                double colCard = (double) tablesInInputFilesMap.get(tableName).columns.get(colId).colCardinality;
                ff = 1/colCard;
                break;
            }
        }
        return ff;
    }

    public static void populateTable2Type(List<Predicate> table_2_rows, List<String> predicate_text_OR, List<String> predicate_list, String data ){
        if(data.contains(" OR ") && predicate_text_OR.size() == 1){
            table_2_rows.get(0).type = "I";
        }else{
            for(int i=0; i<predicate_list.size();i++){
                boolean isEqual = true;
                String predValue = predicate_list.get(i);
                if(predValue.contains(">") || predValue.contains("<")){
                    isEqual = false;
                }
                String ans = "";
                if(isEqual){
                    ans = "E";
                }else{
                    ans = "R";
                }
                table_2_rows.get(i).type = ans; 
            }
        }
    }

    public static void populateTable2Cardinality(List<Predicate> table_2_rows, List<String> predicate_text_OR, List<String> predicate_list, String data ){
        if(data.contains(" OR ")){
            for(int i=0; i<predicate_text_OR.size();i++){
                String predValue = predicate_text_OR.get(i).replaceAll(">", "=").replaceAll("<", "=");
                String input = predValue.split("OR")[0].split("=")[0].trim();
                String[] parts = input.split("\\.");
                String tableName = parts[0];
                int colId = Integer.parseInt(parts[1]);
                table_2_rows.get(i).c1 = tablesInInputFilesMap.get(tableName).columns.get(colId-1).colCardinality + "";
                
            }
        }else{
            for(int i=0; i<predicate_list.size();i++){
                String predValue = predicate_list.get(i).replaceAll(">", "=").replaceAll("<", "=");
                if(predValue.split("=")[1].contains(".")){
                    String parts[] = predValue.split("=");
                    String part0[] = parts[0].split("\\.");
                    String part1[] = parts[1].split("\\.");
                    String tableName1 = part0[0].trim();
                    String tableName2 = part1[0].trim();
                    int col1Id = Integer.parseInt(part0[1].trim());
                    int col2Id = Integer.parseInt(part1[1].trim());
                    table_2_rows.get(i).c1 = tablesInInputFilesMap.get(tableName1).columns.get(col1Id-1).colCardinality + "";
                    table_2_rows.get(i).c2 = tablesInInputFilesMap.get(tableName2).columns.get(col2Id-1).colCardinality + "";
                }else{
                    String parts[] = predValue.split("=")[0].split("\\.");
                    String tableName = parts[0].trim();
                    int colId = Integer.parseInt(parts[1].trim()); 
                    table_2_rows.get(i).c1 = tablesInInputFilesMap.get(tableName).columns.get(colId-1).colCardinality + "";
                }
            }
        }
    }

    public static void first_table(HashMap<String, String> table_1_col2){
        
        TableGenerator tableGenerator = new TableGenerator();

        List<String> headersList = new ArrayList<>(); 
        headersList.add("Plan Table");
        headersList.add("Value");
        headersList.add("Description - Possible values");
        

        List<List<String>> rowsList = new ArrayList<>();

        String[] first_col= {"QBlockNo","AccessType","MatchCols","AccessName","IndexOnly","Prefetch","SortC_OrderBy","Table1Card","Table2Card","LeadingTable"};
        String[] third_col= {"Always 1 since we have only 1 block","R - TS scan; I - Index Scan; N - IN list index scan","Number of matched columns in the INDEX key \nwhere ACCESSTYPE is I or N","Name of index if ACCESSTYPE is I or N","Y or N","Blank - no prefetch; S - sequential prefetch","Y or N","Table 1 Cardinality","Table 2 Cardinality","Table name of the outer/composite table in NLJ"};
        for (int i = 0; i < 10; i++) {
            List<String> row = new ArrayList<>(); 
            row.add(first_col[i]);
            row.add(table_1_col2.get(first_col[i]));
            row.add(third_col[i]);
            
            rowsList.add(row);
        }

        System.out.println(tableGenerator.generateTable(headersList, rowsList));

    }
    
    public static void second_table(List<Predicate> table_2_rows){
        
        TableGenerator tableGenerator = new TableGenerator();

        List<String> headersList = new ArrayList<>(); 
        headersList.add("Predicate Table ");
        headersList.add("Type");
        headersList.add("C1");
        headersList.add("C2");
        headersList.add("FF1");
        headersList.add("FF2");
        headersList.add("Seq");
        headersList.add("Text");
        headersList.add("Description - Possible values\nType - E (equal), R (Range), I (IN List)\nC1 - column cardinality left hand side \n(-1 if table is empty)\nC2 - column cardinality right hand side \n(join predicate only)\nFF 1 - Estimate FF for left hand side (-1 if table is empty)\nFF 2 - Estimate FF for right hand side (join predicate only)\nSeq - either 1,2,3,4,5;  the order of each\npredicate is being evaluated\nText - the original text of the predicate");
        

        List<List<String>> rowsList = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            // System.out.println("###############################");
            // System.out.println(seq.indexOf(table_2_rows.get(i).text)+1);
            // System.out.println(table_2_rows.get(i).text);
            // System.out.println(seq.toString());
            List<String> row = new ArrayList<>();
            row.add(0,"PredNo "+(Integer.toString(i+1)));
            row.add(1, table_2_rows.get(i).type);
            row.add(2, table_2_rows.get(i).c1);
            row.add(3, table_2_rows.get(i).c2);
            row.add(4, table_2_rows.get(i).ff1);
            row.add(5, table_2_rows.get(i).ff2);
            row.add(6, table_2_rows.get(i).text.equals("")?"":""+(seq.indexOf(table_2_rows.get(i).text)+1));
            row.add(7, table_2_rows.get(i).text);
            row.add(8, table_2_rows.get(i).description);
            rowsList.add(row);
        }

        System.out.println(tableGenerator.generateTable(headersList, rowsList));

    }
    
    public static void create_index(String data){
        StringTokenizer st = new StringTokenizer(data," "); 
        ArrayList<String> index_order = new ArrayList<>();
        ArrayList<Integer> index_col_number = new ArrayList<>();
        
        // System.out.println(data);
        int index=0;
        String index_name="";
        String table_name="";
        while (st.hasMoreTokens()) { 

            String s=st.nextToken();
            index++;
            switch(index){
                case 3:
                // System.out.println(s);
                index_name=s;
                break;
                case 5:
                // System.out.println(s);
                table_name=s;
                break;
                default:
                if(index>5){
                    if (!(s.equals(",") || s.equals(")") || s.equals("("))){
                        index_order.add(s.substring(s.length()-1));
                        index_col_number.add(Integer.parseInt(s.substring(0,s.length()-1)));
                    }
                }
            }
        }
        // System.out.println(index_name+"  "+table_name);
        // System.out.println(index_order);
        // System.out.println(index_col_number);

        get_table_data(table_name+".tab");

        set_index_data(index_order,index_col_number,index_name,table_name);
    }

    public static void list_index(String data){
        int number_of_col=5;

        StringTokenizer st = new StringTokenizer(data," ");     
        st.nextToken();
        st.nextToken();
        String table_name=st.nextToken();

        System.out.print(padRight("Index File Name",file_name_padding));
        for(int i=0; i<number_of_col;i++){
            System.out.print(padRight("Column"+Integer.toString(i+1), column_padding));
        }
        System.out.println("");
        
        for(int i=0;i<(number_of_col*column_padding)+file_name_padding;i++ ){
            System.out.print("_");
        }
        System.out.println("");

        File curDir = new File(".");
        File[] filesList = curDir.listFiles();
        for(File f : filesList){
            String name=f.getName();
            if(f.isFile() && name.substring(name.length()-3).equals("idx") && name.substring(0,table_name.length()).equals(table_name)){
                print_row(name,null);
            }
        }
        System.out.println("\n\n");
    }
    
    public static ArrayList<String> list_index_return(String table_name){
        
        ArrayList<String> temp= new ArrayList<>();
        
        File curDir = new File(".");
        File[] filesList = curDir.listFiles();
        for(File f : filesList){
            String name=f.getName();
            if(f.isFile() && name.substring(name.length()-3).equals("idx") && name.substring(0,table_name.length()).equals(table_name)){
                temp.add(name);
            }
        }
        return temp;
    }

    public static void drop_index(String data){
        StringTokenizer st = new StringTokenizer(data," ");     
        st.nextToken();
        st.nextToken();
        File myObj = new File(st.nextToken()+".idx"); 
        if (myObj.delete()) { 
        System.out.println("Deleted the file: " + myObj.getName());
        } else {
        System.out.println("Failed to delete the file.");
        } 
    }

    public static void set_index_data(ArrayList<String> index_order,ArrayList<Integer> index_col_number,String index_name,String table_name){
        ArrayList<String> index_data = new ArrayList<>();
        String first_col="";
        for( int i=0;i<index_order.size();i++){
            first_col+=Integer.toString(index_col_number.get(i)) + index_order.get(i) +" ";
        }
        int index_row_num=0;
        for(ArrayList<String> row : columnData){
            index_row_num++;
            String index_row="";
            for( int i=0;i<index_col_number.size();i++){
                // System.out.println(row);
                // System.out.println(index_col_number.get(i));
                
                if(index_order.get(i).equals("A"))
                {
                    index_row+=row.get(index_col_number.get(i)-1)+" ";
                }
                else{
                    index_row+=reverse(row.get(index_col_number.get(i)-1))+" ";
                }
            }
            index_row+=";"+Integer.toString(index_row_num);
            index_data.add(index_row);
        }
        // System.out.println(index_data);

        Collections.sort(index_data,String.CASE_INSENSITIVE_ORDER);
        index_data.add(0,Integer.toString(columnData.size()));
        index_data.add(0,first_col);
        // System.out.println(index_data);
        
        
        try {
            FileWriter myWriter = new FileWriter(table_name+index_name+".idx");
            int ind=0;
            // System.out.println("File writing");
            for(String row_data: index_data){
                if(ind==0 || ind==1){
                    // System.out.println("First 2 rows");
                    myWriter.write(row_data+"\n");
                    ind++;
                    continue;
                }
                // System.out.println("Next rows");
                // System.out.println(row_data);
                StringTokenizer st = new StringTokenizer(row_data,";"); 
                String ind_data=st.nextToken();
                // System.out.println(ind_data);
                String col_num=st.nextToken();
                // System.out.println(col_num);
                String data=col_num+" '"+ind_data+"'\n";
                myWriter.write(data);
                ind++;
            }
            myWriter.close();
            // System.out.println("Successfully wrote to the file.");
          } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
          }

           
    }
    
    public static  void get_table_data(String filename){

        columnType = new ArrayList<>();
        columnTypeLen = new ArrayList<>();
        columnCard = new ArrayList<>();
        columnData = new ArrayList<ArrayList<String>>();

        int lineNumber=0;
        try 
        {
            File myObj = new File(filename);
            Scanner myReader = new Scanner(myObj);
            int number_of_Rows = 0;
            
            while (myReader.hasNextLine()) {
                lineNumber++;
                String data = myReader.nextLine();
                StringTokenizer st = new StringTokenizer(data," ");  
                
                if (lineNumber==1){
                    while (st.hasMoreTokens()) { 

                        String s=st.nextToken();
                        if (s.charAt(0)=='C'){
                            int len=0;
                            len=Integer.parseInt(s.substring(1, s.length()));
                            columnTypeLen.add(len);
                        }
                        else{
                            columnTypeLen.add(-1);
                        }
                        columnType.add(s); 
                    }
                   continue; 
                }
                else if (lineNumber==2){
                    while (st.hasMoreTokens()) {  
                        columnCard.add(Integer.parseInt(st.nextToken())); 
                    }
                   continue;
                }
                else if (lineNumber==3){
                    number_of_Rows=Integer.parseInt(data);
                    continue;
                }
                ArrayList<String> temp=new ArrayList<String>();
                int index =0;
                while (st.hasMoreTokens()) {
                    int t=columnTypeLen.get(index);
                    
                    String s=st.nextToken();
                    if ( t>0){
                        int t1=t-s.length();
                        for(int j=0;j<t1;j++){
                            s+=" ";
                        }
                    }
                    else{
                        int t1=10-s.length();
                        for(int j=0;j<t1;j++){
                            s="0"+s;
                        }
                    }
                    
                    temp.add(s);   
                    index++;
                }
                // System.out.println(temp);
                columnData.add(temp);
            }

            myReader.close();
          } 
          catch (FileNotFoundException e) 
          {
            System.out.println("An error occurred.");
            e.printStackTrace();  
          }
 

    }

    public static String reverse(String s)
    {
        String decoded = "";
        String alphabet = "abcdefghijklmnopqrstuvwxyz";
        String alphabet_caps = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String alphabet_caps_rev = "ZYXWVUTSRQPONMLKJIHGFEDCBA";
        
        String alphabet2 = "zyxwvutsrqponmlkjihgfedcba";
        String number1 = "0123456789";
        String number2 = "9876543210";

        for (char c : s.toCharArray()) {
            if ((c < 'a' || c > 'z') && (c < '0' || c>'9') && (c < 'A' || c > 'Z') ) {
                decoded += c; 
            } 
            else if (c >= 'a' && c <= 'z')
            {
                int pos = alphabet.indexOf(c);
                decoded += alphabet2.charAt(pos);               
            }
            else if (c >= 'A' && c <= 'Z')
            {
                int pos = alphabet_caps.indexOf(c);
                decoded += alphabet_caps_rev.charAt(pos);               
            }
            else 
            {
                // System.out.println(c);
                int pos = number1.indexOf(c);
                decoded += number2.charAt(pos);               
            }
        }
        return decoded;
    }

    public static void print_row(String name, Table t){
        System.out.print(padRight(name.substring(0,name.length()-4),file_name_padding));
        

        try {
            File myObj = new File(name);
            Scanner myReader = new Scanner(myObj);
            StringTokenizer st2 = new StringTokenizer(myReader.nextLine()," ");
            String key="";
            for( int i=0;i<5;i++){

                if (st2.hasMoreTokens()){
                    if(i==0){
                        key=st2.nextToken();
                        System.out.print(padRight(key,column_padding));
                    }
                    else
                    System.out.print(padRight(st2.nextToken(),column_padding));
                }
                else{
                    System.out.print(padRight("-",column_padding));
                }
            }
            if(t!=null){
                key=key.substring(0, key.length()-1);
                String h2k="-", l2k="-";
                // System.out.println(key);
                for(Column col: t.columns){
                    // System.out.print(col.colId);
                    if (Integer.parseInt(key)==col.colId+1){
                        h2k=col.high2Key;
                        l2k=col.low2Key;
                    }
                }
                System.out.print(padRight(""+h2k,column_padding));
                System.out.print(padRight(""+l2k,column_padding));
            }
            System.out.println("");
            myReader.close();   
        }
        catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();  
        }
        

    }

    public static String padRight(String s, int n) {
        return String.format("%-" + n + "s", s);  
    }
   
    public static String padLeft(String s, int n) {
        return String.format("%" + n + "s", s);  
    }

    public static void extractTablesFromFiles(Map<String, Table> tablesInFiles){
        File f = new File("./");
        File[] files = f.listFiles();
        for (int i = 0; i < files.length; i++) {
            if(files[i].getName().contains(".tab")){
                String tableName = files[i].getName().replaceAll(".tab", "");
                int numberOfRows;
                try{
                    List<Column> columns = new ArrayList<>();
                    Scanner myReader = new Scanner(files[i]);
                    String[] dataTypes = myReader.nextLine().split(" ");
                    String[] colCards = myReader.nextLine().split(" ");

                    List<List<String>> values = new ArrayList<>();
                    for(int k=0; k<dataTypes.length;k++){
                        values.add(new ArrayList<>());
                    }

                    numberOfRows = Integer.parseInt(myReader.nextLine());
                    for(int k=0; k<dataTypes.length;k++){
                        String dataType = dataTypes[k].contains("C")? "C" : "I";
                        int colCard = Integer.parseInt(colCards[k]) == -1? 25 : Integer.parseInt(colCards[k]);
                        Column col = new Column(k,dataType,colCard);
                        
                        columns.add(col);
                    }
                    while(myReader.hasNext()){
                        String[] data = myReader.nextLine().split(" ");
                        // System.out.println(data.length);
                        // for (String s: data)
                        //     System.out.println(s);
                        for(int m=0; m<data.length; m++){
                            values.get(m).add(data[m]);
                        }
                    }
                    for(List<String> val : values){
                        Collections.sort(val);
                    }
                    for(int k=0; k<columns.size();k++){
                        List<String> temp = values.get(k);
                        columns.get(k).low2Key = temp.get(0);
                        columns.get(k).high2Key = temp.get(0);
                        if(temp.size() > 0){
                            columns.get(k).high2Key = temp.get(temp.size()-1);
                        }
                    }

                    tablesInFiles.put(tableName, new Table(tableName, numberOfRows, columns));
                    myReader.close();
                }catch(Exception e){
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }
    }

    public static void orparsing(String s, ArrayList<String> list_values, ArrayList<String> check_pred,List<String> predicate_text_OR, HashMap<String, ArrayList<String>> outmap) {
        String[] from = s.split("FROM");
        String tb_part = from[1];
        tb_part = tb_part.trim();
        if (tb_part.contains("WHERE")) {
            if (tb_part.contains("OR")) {
                if (tb_part.contains("ORDER BY")) {
                    String[] temp = tb_part.split("ORDER BY");
                    tb_part = temp[0];
                }
                String[] temp = tb_part.split("WHERE");
                tb_part = temp[1].trim();
                // System.out.println(tb_part);
                String[] parts = tb_part.split(" ");
                for (int i = 0; i < parts.length; i++) {
                    if (predicate_type.contains(parts[i])) {
                        list_values.add(parts[i - 1] + " " + parts[i] + " " + parts[i + 1]);
                    }
                }
                for (int i = 0; i < list_values.size(); i++) {
                    for (int j = i + 1; j < list_values.size(); j++) {
                        if (list_values.get(i).substring(0, 4).equals(list_values.get(j).substring(0, 4))) {
                            String[] tempor1 = list_values.get(i).split("=");
                            String[] tempor2 = list_values.get(j).split("=");
                            ArrayList<String> temp2 = outmap.getOrDefault(tempor1[0].trim(), new ArrayList<String>());
                            temp2.add(tempor1[1]);
                            outmap.put(tempor1[0].trim(), temp2);
                            ArrayList<String> temp3 = outmap.getOrDefault(tempor2[0].trim(), new ArrayList<String>());
                            temp3.add(tempor2[1]);
                            outmap.put(tempor2[0].trim(), temp3);

                        }
                    }
                }
                for (String key : outmap.keySet()) {
                    String inputPred = "";

                    for (String value : outmap.get(key)) {
                        inputPred += key + " = ";
                        inputPred += value + " OR ";
                    }
                    inputPred = inputPred.substring(0, inputPred.length() - 4);
                    predicate_text_OR.add(inputPred);
                    inputPred = "";
                }
                for (String list_value : list_values) {
                    String sub = list_value.substring(0, 4);
                    if (!outmap.keySet().contains(sub)) {
                        predicate_text_OR.add(list_value);
                    }
                }
                // System.out.println("Map keyset:" + outmap.keySet().toString());
                // System.out.println("ANS:" + predicate_text_OR.toString());
            }
        }
        }

}

class FilterFactor{
    double ff1;
    double ff2;

    public FilterFactor(double ff1, double ff2){
        this.ff1 = ff1;
        this.ff2 = ff2;
    }
}
    
class Table{
    String tableName;
    int numberOfRows;
    List<Column> columns;

    public Table(String tableName, int numberOfRows, List<Column> columns){
        this.tableName = tableName;
        this.numberOfRows = numberOfRows;
        this.columns = columns;
    }
    public String toString(){
        return "Table :"+tableName + " Card : "+ numberOfRows + columns.toString() ;
    }

}

class Predicate{
    String type;
    String c1;
    String c2;
    String ff1;
    String ff2;
    String seq;
    String text;
    String description;

    public Predicate(){
        this.type = "";
        this.c1 = "";
        this.c2 = "";
        this.ff1 = "";
        this.ff2 = "";
        this.seq = "";
        this.text = "";
        this.description = "";
    }
}

class Column{
    int colId;
    String dataType;
    int colCardinality;
    String high2Key;
    String low2Key;

    public Column(int colId, String dataType, int colCardinality){
        this.colId = colId;
        this.dataType = dataType;
        this.colCardinality = colCardinality;
        this.high2Key = "";
        this.low2Key = "";
    }
    public String toString(){
        return "\n    COL :"+colId + " Card : "+colCardinality+ " hkey : "+high2Key+" lkey: "+low2Key ;
    }
}
class Index{
    String index_name;
    ArrayList<String> order;
    int colCardinality;
    ArrayList<Double> filter_factor;
    public Index(String index_name, ArrayList<String> order, int colCardinality,ArrayList<Double> filter_factor){
        this.index_name = index_name;
        this.order = order;
        this.colCardinality = colCardinality;
        this.filter_factor = filter_factor;
    }
}
class Index_matching{
    String index_name;
    ArrayList<String> order;
    ArrayList<String> index_order;
    int num_of_match;
    int total_num;
    ArrayList<Double> filter_factor;
    public Index_matching(String index_name, ArrayList<String> order,int num_of_match,int total_num,ArrayList<Double> filter_factor,ArrayList<String> index_order){
        this.index_name = index_name;
        this.order = order;
        this.index_order = index_order;
        this.num_of_match=num_of_match;
        this.total_num=total_num;
        this.filter_factor = filter_factor;
    }
    public String toString(){
        // System.out.print();
        return this.index_name+" "+this.num_of_match+" "+this.total_num;
    }
}


class TableGenerator {

    private int PADDING_SIZE = 1;
    private int max_with_common_var = 60;
    private String NEW_LINE = "\n";
    private String TABLE_JOINT_SYMBOL = "-";
    private String TABLE_V_SPLIT_SYMBOL = "|";
    private String TABLE_H_SPLIT_SYMBOL = "-";

    public String generateTable(List<String> headersList, List<List<String>> rowsList,int... overRiddenHeaderHeight)
    {
        StringBuilder stringBuilder = new StringBuilder();

        int rowHeight = overRiddenHeaderHeight.length > 0 ? overRiddenHeaderHeight[0] : 1; 

        Map<Integer,Integer> columnMaxWidthMapping = getMaximumWidhtofTable(headersList, rowsList);

        stringBuilder.append(NEW_LINE);
        stringBuilder.append(NEW_LINE);
        createRowLine(stringBuilder, headersList.size(), columnMaxWidthMapping);
        stringBuilder.append(NEW_LINE);


        for (int headerIndex = 0; headerIndex < headersList.size(); headerIndex++) {
            if(headersList.get(headerIndex).contains("\n")){
                StringTokenizer st= new StringTokenizer(headersList.get(headerIndex),"\n");
                fillCell(stringBuilder, st.nextToken(), headerIndex, columnMaxWidthMapping);
                stringBuilder.append(NEW_LINE);
                while(st.hasMoreTokens()){
                    for (int headerIndex_inner = 0; headerIndex_inner < headersList.size()-1; headerIndex_inner++) {
                        fillCell(stringBuilder, " ", headerIndex_inner, columnMaxWidthMapping);
                    }
                    fillCell(stringBuilder, st.nextToken(), headerIndex, columnMaxWidthMapping);
                    stringBuilder.append(NEW_LINE);
                }
            }
            else{
                fillCell(stringBuilder, headersList.get(headerIndex), headerIndex, columnMaxWidthMapping);
            }
        }
        if (stringBuilder.charAt(stringBuilder.length() - 1)=='\n'){
            stringBuilder.setLength(stringBuilder.length() - 1);
        }
        
        stringBuilder.append(NEW_LINE);

        createRowLine(stringBuilder, headersList.size(), columnMaxWidthMapping);


        for (List<String> row : rowsList) {

            for (int i = 0; i < rowHeight; i++) {
                stringBuilder.append(NEW_LINE);
            }

            for (int cellIndex = 0; cellIndex < row.size(); cellIndex++) {
                if(row.get(cellIndex).contains("\n")){
                    StringTokenizer st= new StringTokenizer(row.get(cellIndex),"\n");
                    fillCell(stringBuilder, st.nextToken(), cellIndex, columnMaxWidthMapping);
                    stringBuilder.append(NEW_LINE);
                    while(st.hasMoreTokens()){
                        for (int headerIndex_inner = 0; headerIndex_inner < headersList.size()-1; headerIndex_inner++) {
                            fillCell(stringBuilder, " ", headerIndex_inner, columnMaxWidthMapping);
                        }
                        fillCell(stringBuilder, st.nextToken(), cellIndex, columnMaxWidthMapping);
                        // stringBuilder.append(NEW_LINE);
                    }
                }
                else{
                    fillCell(stringBuilder, row.get(cellIndex), cellIndex, columnMaxWidthMapping);
                }
            }
            stringBuilder.append(NEW_LINE);
            createRowLine(stringBuilder, headersList.size(), columnMaxWidthMapping);

        }

        stringBuilder.append(NEW_LINE);
        // createRowLine(stringBuilder, headersList.size(), columnMaxWidthMapping);
        stringBuilder.append(NEW_LINE);
        stringBuilder.append(NEW_LINE);

        return stringBuilder.toString();
    }

    private void fillSpace(StringBuilder stringBuilder, int length)
    {
        for (int i = 0; i < length; i++) {
            stringBuilder.append(" ");
        }
    }

    private void createRowLine(StringBuilder stringBuilder,int headersListSize, Map<Integer,Integer> columnMaxWidthMapping)
    {
        for (int i = 0; i < headersListSize; i++) {
            if(i == 0)
            {
                stringBuilder.append(TABLE_JOINT_SYMBOL);   
            }

            for (int j = 0; j < columnMaxWidthMapping.get(i) + PADDING_SIZE * 2 ; j++) {
                stringBuilder.append(TABLE_H_SPLIT_SYMBOL);
            }
            stringBuilder.append(TABLE_JOINT_SYMBOL);
        }
    }

    private Map<Integer,Integer> getMaximumWidhtofTable(List<String> headersList, List<List<String>> rowsList)
    {
        Map<Integer,Integer> columnMaxWidthMapping = new HashMap<>();

        for (int columnIndex = 0; columnIndex < headersList.size(); columnIndex++) {
            columnMaxWidthMapping.put(columnIndex, 0);
        }

        for (int columnIndex = 0; columnIndex < headersList.size(); columnIndex++) {

            if(headersList.get(columnIndex).length() > columnMaxWidthMapping.get(columnIndex))
            {
                columnMaxWidthMapping.put(columnIndex, headersList.get(columnIndex).length()>=max_with_common_var?max_with_common_var:headersList.get(columnIndex).length());
            }
        }


        for (List<String> row : rowsList) {

            for (int columnIndex = 0; columnIndex < row.size(); columnIndex++) {

                if(row.get(columnIndex).length() > columnMaxWidthMapping.get(columnIndex))
                {
                    columnMaxWidthMapping.put(columnIndex, row.get(columnIndex).length()>=max_with_common_var?max_with_common_var:row.get(columnIndex).length());
                }
            }
        }

        for (int columnIndex = 0; columnIndex < headersList.size(); columnIndex++) {

            if(columnMaxWidthMapping.get(columnIndex) % 2 != 0)
            {
                columnMaxWidthMapping.put(columnIndex, columnMaxWidthMapping.get(columnIndex) + 1);
            }
        }


        return columnMaxWidthMapping;
    }

    private int getOptimumCellPadding(int cellIndex,int datalength,Map<Integer,Integer> columnMaxWidthMapping,int cellPaddingSize)
    {
        if(datalength % 2 != 0)
        {
            datalength++;
        }

        if(datalength < columnMaxWidthMapping.get(cellIndex))
        {
            cellPaddingSize = cellPaddingSize + (columnMaxWidthMapping.get(cellIndex) - datalength) / 2;
        }

        return cellPaddingSize;
    }

    private void fillCell(StringBuilder stringBuilder,String cell,int cellIndex,Map<Integer,Integer> columnMaxWidthMapping)
    {

        int cellPaddingSize = getOptimumCellPadding(cellIndex, cell.length(), columnMaxWidthMapping, PADDING_SIZE);

        if(cellIndex == 0)
        {
            stringBuilder.append(TABLE_V_SPLIT_SYMBOL); 
        }

        // fillSpace(stringBuilder, cellPaddingSize);
        stringBuilder.append(cell);
        if(cell.length() % 2 != 0)
        {
            stringBuilder.append(" ");
        }

        fillSpace(stringBuilder, cellPaddingSize*2);

        stringBuilder.append(TABLE_V_SPLIT_SYMBOL); 

    }

}
