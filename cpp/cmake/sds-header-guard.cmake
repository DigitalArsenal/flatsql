# SDS files deliberately share the basename main.fbs. Give generated C++
# headers distinct guards; schema bytes and generated data accessors are intact.
file(READ "${HEADER}" CONTENT)
string(REPLACE "FLATBUFFERS_GENERATED_MAIN_H_" "FLATSQL_SDS_${TABLE}_GENERATED_H_" CONTENT "${CONTENT}")
file(WRITE "${HEADER}" "${CONTENT}")
