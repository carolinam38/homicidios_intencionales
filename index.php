<?php

$url= array('https://www.datosabiertos.gob.ec/dataset/homicidios-intencionales/resource/cb8f704e-2b27-4d7f-9431-d40c4e27fa48',
'https://www.datosabiertos.gob.ec/dataset/homicidios-intencionales/resource/36b055c8-e10c-4e57-ba25-3046ca5ef15d');

foreach ($url as $ruta) {

    $arrContextOptions=array(
        "ssl"=>array(
            "verify_peer"=>false,
            "verify_peer_name"=>false,
        ),
    );

    $data = trim((file_get_contents($ruta, false, stream_context_create($arrContextOptions))));//strip_tags
    preg_match_all('/<(.+?)[\s]*\/?[\s]*>/si', $data, $salida);
    $tags = array_unique($salida[1]);
    $arrDatos = [];

    if(count($tags)>0) {
        foreach ($tags as $value) {
            if(strpos(strval(trim($value)),"href=")!==false && strpos(strval(trim($value)),".xls")!==false) {
                $value = str_replace('a href="', '', $value);
                $value = str_replace('" class="btn btn-sinopia"', "", $value);
                $arrDatos[] = $value;
            }
        }
    }
    
    if(count($arrDatos)>0) {
        foreach($arrDatos as $value) {
            if (file_put_contents(basename($value), file_get_contents($value))) {
                echo "File downloaded successfully. <br>";
            } else {
                echo "File downloading failed. <br>";
            }
        }
    }
}
?>