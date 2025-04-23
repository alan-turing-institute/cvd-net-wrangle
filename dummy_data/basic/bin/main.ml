open Types
open Utilities

let generate_patient (id:int) (age:float) (sex:sex) : patient = 
    {id; age; sex}

let generate_patients (n:int) : patient list =
    let sample_age = gaussian ~mu:50.0 ~sigma:10.0 in
    List.init n (fun i -> 
        let sex = sex_of_index (random_int ~min:0 ~max:count_sexes) in
        let age = sample_age () in
        generate_patient (i+1) age sex
    )

let () =
    Random.init 10;
    let patients = generate_patients 10 in
    List.iter (fun p -> Printf.printf "ID:%i \t   age:%.1f \t  sex:%s\n" p.id p.age (string_of_sex p.sex)) @@ patients;

    


