package structs

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// TODO: this needs to be moved to it's own package i guess ?
//  along with utilities for stripping comments and trailing commas from json
type PBTags struct {
	Id         int
	Type       string
	Name       string
	IsRequired bool
	DefValue   string
}

func (p *PBTags) IsValid() bool {
	return 0 != p.Id
}

func PBTagsFromString(tags string) (p *PBTags) {
	p = &PBTags{}

	if "" == tags {
		return
	}

	v := strings.Split(tags, ",")
	if len(v) < 3 {
		return
	}

	p.Type = v[0]
	p.Id, _ = strconv.Atoi(v[1])
	p.IsRequired = (v[2] == "req")

	for i := 3; i < len(v); i++ {
		kv := strings.SplitN(v[i], "=", 2)

		switch kv[0] {
		case "name":
			p.Name = kv[1]
		case "def":
			p.DefValue = kv[1]
		}
	}

	return p
}

// walks all struct fields and checks `protobuf` struct tags for required fields
// if any field is marked as required by `protobuf` tag and is not set - we complain
// NOTE: since with gogoprotobuf some fields can be marked with "(gogoproto.nullable) = false"
// it's impossible to check if those were not set in config or set to zero value, this function only handles pointer fields
func PBCheckRequiredFields(value interface{}) (err error) {
	return WalkStructFieldsRecursive(reflect.ValueOf(value),
		func(parent reflect.Value, fieldNo int) error {
			pt := parent.Type()
			t := pt.Field(fieldNo)
			v := parent.Field(fieldNo)

			tags := PBTagsFromString(t.Tag.Get("protobuf"))
			if tags == nil || !tags.IsRequired {
				return nil
			}

			// field is set = all good, skip it
			if !v.IsNil() {
				return nil
			}

			// required field is nil here = we need to return an error

			// try construct the best error message we can (take json field name and path)
			jsonName := func() string {
				jtag := t.Tag.Get("json")
				jname := strings.Split(jtag, ",")[0] // json name for that field

				if jname == "" || jname == "-" {
					return t.Name
				}
				return jname
			}()

			// TODO(antoxa): we can do better than reporting just the parent type name here (i.e. reconstruct 'full path' in json)
			return fmt.Errorf("required field \"%s\" missing in %s", jsonName, pt.Name())
		})
}
