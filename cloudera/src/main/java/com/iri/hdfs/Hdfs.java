package com.iri.hdfs;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.util.*;

public class Hdfs {
	
	public static String nombreArchivo;
	public static String contenido = "contenido del archivo";
	
	public static String usuario;
	public static String grupo;
	public static String permissions;
	
	public static String route;
	
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		
		//Creamos la configuración de acceso al HDFS
		Configuration conf = new Configuration(true);
		conf.set("fs.defaultFS", "hdfs://10.0.2.15:8020/");
		
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		
		Scanner menu = new Scanner (System.in);
		boolean salir = false;
		int opcion; //Guardaremos la opcion del usuario
		 
        //creamos mensajes para mostrar menu
		while (!salir) {
 
            System.out.println("Para crear un archivo preione 1");
            System.out.println("Para borrar un archivo preione 2");
            System.out.println ("Para salir presione 3");
            
            try {
            	//pedimos ingresar la opcion que desea 
                System.out.println("Escribe una de las opciones");
                opcion = menu.nextInt();
 
                //se crea el menu mediante el metodo switch
                switch (opcion) {
                    case 1:
            			//Crear objeto FileSystem
            			FileSystem fs = FileSystem.get(conf);
            			
            			System.out.println("Escribe el nombre del archivo");
            	    	Scanner nombrearchivo = new Scanner(System.in);
            	    	nombrearchivo.nextLine();
            	    	nombreArchivo=nombrearchivo.toString();
            	    	System.out.println("¿En que ruta deseas guardar el archivo?");
            	    	Scanner ruta = new Scanner(System.in);
            	    	ruta.nextLine();
            	    	route=ruta.toString();
            			
            			//Si no existe el archivo, hay que crearlo
            			Path rutaArchivo = new Path(route + File.separator + nombreArchivo);
            			FSDataOutputStream outputStream = null;
            			
            			if(!fs.exists(rutaArchivo)) {
            				outputStream = fs.create(rutaArchivo);
            				outputStream.writeBytes(contenido);
            				outputStream.close();
            			}
            			
            			//Tambien podemos modificar el propietario o los permisos del archivo.
            			
            			System.out.println("¿Quien sera el propietario del arvchivo?");
            	    	Scanner user = new Scanner(System.in);
            	    	user.nextLine();
            	    	usuario=user.toString();
            	    	System.out.println("¿Cual sera el grupo propietario del arvchivo?");
            	    	Scanner group = new Scanner(System.in);
            	    	group.nextLine();
            	    	grupo=group.toString();
            			fs.setOwner(rutaArchivo, usuario , grupo);
            			
            			System.out.println("Elija los permisos del archivo");
            	    	Scanner permisos = new Scanner(System.in);
            	    	permisos.nextLine();
            	    	permissions=permisos.toString();
            	    	FsPermission permis = FsPermission.valueOf("-rwxrwxrwx");
            			fs.setPermission(rutaArchivo, permis);
                        break;
                    case 2:
                    	//se crea objeto para borrar el archivo deseado
                    	FileSystem fsd = FileSystem.get(conf);
                    	System.out.println("Escribe la ruta del archivo que deseas borrar");
            	    	Scanner rutad = new Scanner(System.in);
            	    	rutad.nextLine();
            	    	route=rutad.toString();
                    	fsd.delete(new Path(route), true);
            			fsd.close();
                        break;
                    case 3:
                    	//opcion para salir del menu
                    	salir = true;
                        break;
                    default:
                        System.out.println("Seleccione un numero entre 1 y 3");
                }
            } catch (InputMismatchException e) {
                System.out.println("Debes insertar un número");
                menu.next();
            } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
				
        }
    }
}

