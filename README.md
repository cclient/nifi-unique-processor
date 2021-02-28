Nifi Unique Processor
================================

```html
<custom_id:1,custom_value:123>                              <custom_id:1,custom_value:123>
<custom_id:1,custom_value:456> -> unique by ${custom_id}-> 
<custom_id:2,custom_value:789>                              <custom_id:2,custom_value:789>
```

nifi queued distinct/unique by 'custom key'

---

### deploy

#### 1 compile

`mvn package`

#### 2 upload to one of

```nifi

nifi.nar.library.directory=./lib
nifi.nar.library.directory.custom=./lib_custom
nifi.nar.library.autoload.directory=./extensions
nifi.nar.working.directory=./work/nar/

```

cp nifi-unique-nar/target/nifi-unique-nar-0.1.nar nifi/lib_custom/

#### 3 restart nifi if need

nifi/bin/nifi.sh restart