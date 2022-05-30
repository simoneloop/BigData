from django.shortcuts import render

# Create your views here.
def front(request):
    return render(request,'front_spark/test.html')