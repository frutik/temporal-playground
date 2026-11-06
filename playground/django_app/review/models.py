from django.db import models


class Product(models.Model):
    product_id = models.CharField(max_length=64, unique=True)
    title = models.CharField(max_length=255)
    description = models.TextField()
    category = models.CharField(max_length=255)
