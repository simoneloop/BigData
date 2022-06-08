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
  readonly FUNC1="/func2"
  posts:any;
  constructor(private http:HttpClient){}

  getPosts(){
    let params=new HttpParams().set("ciao","ciccio");
    this.http.get(this.ROOT_URL+"/api"+this.FUNC1,{params}).subscribe(data => console.log(data));
  }



}
