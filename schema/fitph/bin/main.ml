(* open Types *)
open Utilities
open Demographic
open Medical_history
open Visit
open Clinical_assessment


let () =
  let tables = demography_tables @ visit_tables @ medical_history_tables @ clinical_assessment_tables in
  let vocabulary = demography_vocabulary in
  let oc = open_out "create_schema.sql" in
  List.iter (fun table -> Printf.fprintf oc "\n\n%s" @@ generate_create_table_sql table) tables;
  List.iter (fun values-> Printf.fprintf oc "\n\n%s" @@ generate_insert_into_table_sql values) vocabulary;
  close_out oc;
  (* let file = open_out "create.sql" in
  let sql = List.map (fun table-> Printf.sprintf "\n\n%s" @@ generate_create_table_sql table ) tables in
  Printf.fprintf oc "The answer is: %d\n" 42; *)
