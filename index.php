<?php
//Contiene dos enlaces a recursos de homicidios intencionales en datos abiertos del Ecuador.
$url= array('https://www.datosabiertos.gob.ec/dataset/homicidios-intencionales/resource/cb8f704e-2b27-4d7f-9431-d40c4e27fa48',
'https://www.datosabiertos.gob.ec/dataset/homicidios-intencionales/resource/36b055c8-e10c-4e57-ba25-3046ca5ef15d');
//Se recorre cada URL en $url para obtener su contenido y buscar los enlaces a archivos .xls
foreach ($url as $ruta) {
//Desactivación de la verificación SSL
    $arrContextOptions=array(
        "ssl"=>array(
            "verify_peer"=>false,
            "verify_peer_name"=>false,
        ),
    );
//obtiene el contenido HTML de la URL
    $data = trim((file_get_contents($ruta, false, stream_context_create($arrContextOptions))));//strip_tags
//Se usa una expresión regular /<(.+?)[\s]*\/?[\s]*>/si para capturar etiquetas HTML
    preg_match_all('/<(.+?)[\s]*\/?[\s]*>/si', $data, $salida);
    $tags = array_unique($salida[1]);
    $arrDatos = [];

    if(count($tags)>0) {
        foreach ($tags as $value) {
            //Se busca la cadena "href=" y ".xls" dentro de las etiquetas capturadas
            if(strpos(strval(trim($value)),"href=")!==false && strpos(strval(trim($value)),".xls")!==false) {
                $value = str_replace('a href="', '', $value);
                $value = str_replace('" class="btn btn-sinopia"', "", $value);
                $arrDatos[] = $value;
            }
        }
    }
    
    if(count($arrDatos)>0) {
        foreach($arrDatos as $value) {
           // Se usa para guardar los archivos localmente.
            if (file_put_contents(basename($value), file_get_contents($value))) {
                echo "File downloaded successfully. <br>";
            } else {
                echo "File downloading failed. <br>";
            }
        }
    }
}
?>
