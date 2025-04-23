
type sex = Male | Female | Missing
let count_sexes = List.length [Male;Female]
let sex_of_index (i:int) : sex = 
match i with
  | 0 -> Male
  | 1 -> Female
  | _ -> Missing

type patient = {
  id: int;
  age: float;
  sex: sex;
}
let string_of_sex (s:sex):string=
  match s with
  | Male -> "Male"
  | Female -> "Female"
  | Missing -> "Missing"