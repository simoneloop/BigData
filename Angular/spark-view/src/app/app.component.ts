import { Component } from '@angular/core';
import {HttpClient, HttpParams} from '@angular/common/http';
import { Observable } from 'rxjs';
import { debuglog } from 'util';
@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  title = 'spark-view';

  readonly ROOT_URL='http://localhost:8080';
  readonly FUNC1="/func1"
  readonly FUNC2="/func2"
  readonly FUNC3="/func3"
  res:any;

  response:any;
  constructor(private http:HttpClient){}

  getJson(){
    let params = new Map<string, any>();
    params.set("field1",'ciccio');
    params.set('field2',"ciao")
    let httpParams=this.getParams(params)
    /* let fParams=undefined; */
    /* let httpParams = new HttpParams();
    params.forEach((value, key) => { httpParams=httpParams.set(key,value) } )
    console.log(httpParams) */
    let url=this.ROOT_URL+this.FUNC1+(httpParams?"/?"+httpParams:"")
    if(httpParams)url=url.substring(0,url.length-1)
    this.http.get(url).subscribe(data => {this.response=data;console.log(this.response)});
  }

  getParams(params:Map<String,any>){
    let res="";
    if(params && params.size>0){
      params.forEach((value, key) => { res+=key+"="+value+"&" } );
      return res;
    }
    else{return undefined;}

  }



}
