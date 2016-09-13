package structs

import (
	"badoo/_packages/util/deepcopy"

	"fmt"
	"reflect"
	"strings"
)

type BadooTags struct {
	IsSecret  bool
	SecretVal string
}

func BadooTagsFromString(tag string) *BadooTags {
	result := &BadooTags{}

	if tag == "" {
		return nil
	}

	v := strings.Split(tag, ",")
	if len(v) < 1 {
		return nil
	}

	vv := strings.Split(v[0], "=")
	if vv[0] == "secret" {
		result.IsSecret = true
	}
	if len(vv) == 2 {
		result.SecretVal = vv[1]
	} else {
		result.SecretVal = "*******"
	}

	return result
}

// walks the struct and zeroes/resets all fields that are marked with `badoo:"secret"` tags
func BadooStripSecretFields(st interface{}) (interface{}, error) {

	v := deepcopy.Iface(st) // copy the value to avoid wrecking the passed one

	err := WalkStructFieldsRecursive(reflect.ValueOf(v),
		func(parent reflect.Value, fieldNo int) error {
			pt := parent.Type()
			t := pt.Field(fieldNo)
			v := parent.Field(fieldNo)

			tags := BadooTagsFromString(t.Tag.Get("badoo"))
			if tags == nil || !tags.IsSecret {
				return nil
			}

			// field is empty = no need to reset
			if v.Interface() == reflect.Zero(t.Type).Interface() {
				return nil
			}

			// we MUST set the field, if we can't -> complain
			if !v.CanSet() {
				return fmt.Errorf("field %s::%s.%s is not settable but must be removed", t.PkgPath, pt.Name(), t.Name)
			}

			// now set the secret value
			//  we handle setting to plain strings and string pointers, otherwise - set zero of whatever type the value is
			// XXX: if you redo it as a switch - don't forget to handle the case when it's a pointer to non-string type
			sv := reflect.ValueOf(tags.SecretVal)

			if v.Kind() == reflect.String {
				v.Set(sv)
			} else if v.Kind() == reflect.Ptr && v.Elem().Kind() == reflect.String {
				v.Elem().Set(sv)
			} else {
				v.Set(reflect.Zero(t.Type))
			}

			return nil
		})

	if err != nil {
		return nil, err
	}

	return v, nil
}
