import { Component } from '@angular/core';
import {HttpClient, HttpParams} from '@angular/common/http';
import { Observable } from 'rxjs';

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
    let params=new HttpParams().set("ciao","ciccio");
    this.http.get(this.ROOT_URL+"/api"+this.FUNC1,{params}).subscribe(data => {this.response=data;});
  }



}